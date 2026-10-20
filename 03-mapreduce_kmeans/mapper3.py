import grpc
from concurrent import futures
import pandas as pd
import numpy as np
import os
import mapreduce_pb2_grpc
import mapreduce_pb2
import time
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    handlers=[logging.FileHandler("dump.txt"),
                              logging.StreamHandler()])

class Mapper(mapreduce_pb2_grpc.MapperServiceServicer):
    def __init__(self):
        self.is_active = False  # Keep track of the mapper's activity

    def ProcessInput(self, request, context):
        self.is_active = True
        logging.info(f"Received mapping task with task ID: {request.task_id}")
        try:
            start_index, end_index = map(int, request.input_data_indices)
            data = pd.read_csv('Data/Input/points.txt', header=None, skiprows=start_index, nrows=end_index-start_index)
            centroids = np.array(request.centroids).reshape(-1, data.shape[1])

            # Calculate distances and determine the closest centroid for each data point
            distances = np.sqrt(((data.to_numpy()[:, np.newaxis] - centroids) ** 2).sum(axis=2))
            closest_centroids = np.argmin(distances, axis=1)

            output_dir = f"Data/Mappers/M{request.task_id}/"
            os.makedirs(output_dir, exist_ok=True)

            # Create files for each reducer
            partition_files = [open(os.path.join(output_dir, f"partition_{i+1}.txt"), 'w') for i in range(len(centroids))]

            # Distribute data to the appropriate partition file
            for point, centroid_index in zip(data.to_numpy(), closest_centroids):
                np.savetxt(partition_files[centroid_index], [point], delimiter=",", fmt='%f')

            # Close all partition files
            for file in partition_files:
                file.close()

            self.is_active = False
            return mapreduce_pb2.MapperResponse(
                success=True,
                message="Data processed successfully",
                intermediate_data_path=output_dir,
                task_id=request.task_id
            )
        except Exception as e:
            logging.error(f"Error in mapping task {request.task_id}: {e}", exc_info=True)
            for file in partition_files:
                file.close()
            self.is_active = False
            return mapreduce_pb2.MapperResponse(
                success=False,
                message=str(e),
                intermediate_data_path="",
                task_id=request.task_id
            )

    def SendData(self, request, context):
        logging.info(f"Attempting to send data for keys {request.keys} from task {request.task_id}")
        response_data = {}
        try:
            task_id = request.task_id  # Ensure task_id is correctly handled
            if not task_id:
                logging.error("Task ID is missing in the request.")
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details('Task ID is required')
                return mapreduce_pb2.FetchResponse(data=response_data, task_id=request.task_id)
            for key in request.keys:
                key_index = int(key)  # Convert key to integer for accessing the partition
                file_path = f"Data/Mappers/M{task_id}/partition_{key_index}.txt"
                logging.debug(f"Looking for file: {file_path}")  # Confirm file path
                try:
                    with open(file_path, 'r') as file:
                        response_data[key] = file.read()
                    logging.info(f"Data for key {key} from task {task_id} sent successfully")
                except FileNotFoundError as fnf_error:
                    logging.error(f"File not found for task {task_id}, key {key}: {fnf_error}")
                    response_data[key] = ""  # Handle file not found by sending empty data
                except Exception as e:
                    logging.error(f"Error reading file for task {task_id}, key {key}: {e}")
                    response_data[key] = ""  # Handle general error
            return mapreduce_pb2.FetchResponse(data=response_data, task_id=request.task_id)
        except Exception as e:
            logging.error(f"General failure in SendData for task {task_id}: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Failed to fetch data for task {task_id}: {str(e)}')
            return mapreduce_pb2.FetchResponse(data=response_data, task_id=request.task_id)

    def GetStatus(self, request, context):
        # Return the current status of the mapper
        return mapreduce_pb2.StatusResponse(is_running=self.is_active, message="Mapper is active" if self.is_active else "Mapper is idle")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapreduce_pb2_grpc.add_MapperServiceServicer_to_server(Mapper(), server)
    server.add_insecure_port('[::]:50053')
    server.start()
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
