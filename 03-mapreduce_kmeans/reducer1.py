import grpc
from concurrent import futures
import time
import logging
import asyncio
import numpy as np
import os
from io import StringIO

import mapreduce_pb2
import mapreduce_pb2_grpc

# Setup logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("reducer.log"), logging.StreamHandler()])

class Reducer(mapreduce_pb2_grpc.ReducerServiceServicer):

    def __init__(self):
        self.mapper_stubs = self.create_mapper_stubs()

    def create_mapper_stubs(self):
        mapper_ports = {
            "mapper1": "50051",
            "mapper2": "50052",
            "mapper3": "50053",
        }
        stubs = {}
        for mapper, port in mapper_ports.items():
            channel = grpc.aio.insecure_channel(f'localhost:{port}')
            stubs[mapper] = mapreduce_pb2_grpc.MapperServiceStub(channel)
        return stubs

    async def FetchData(self, keys, task_id):
        logging.info(f"Fetching data with keys: {keys} for task ID: {task_id}")
        tasks = [stub.SendData(mapreduce_pb2.FetchRequest(keys=keys, task_id=task_id)) for stub in self.mapper_stubs.values()]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        data = {}
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Failed to fetch data from mapper: {result}")
            else:
                data.update(result.data)
        return data

    def validate_and_reshape_data(self, points_str):
        """Convert string data to numpy array, robustly handling errors."""
        try:
            if points_str.strip():
                data_stream = StringIO(points_str)
                return np.genfromtxt(data_stream, delimiter=',').reshape(-1, 2)
            return np.array([], dtype=float).reshape(-1, 2)
        except Exception as e:
            logging.error(f"Error reshaping data: {e}")
            return np.array([], dtype=float).reshape(-1, 2)

    async def ProcessIntermediateData(self, request, context):
        logging.info(f"Received request to process intermediate data for task ID: {request.task_id}")
        try:
            partition_keys = [str(i + 1) for i in range(request.num_reducers)]
            fetched_data = await self.FetchData(partition_keys, request.task_id)
            centroids = []

            for key, points_str in fetched_data.items():
                points = self.validate_and_reshape_data(points_str)
                if points.size > 0:
                    centroid = np.mean(points, axis=0).tolist()
                    centroids.extend(centroid)  # Assuming centroids need to be a flat list

            if centroids:
                return mapreduce_pb2.ReducerResponse(
                    success=True,
                    message="Reduction completed successfully",
                    centroids=centroids,  # Ensure this is correctly formatted according to the .proto definition
                    task_id=request.task_id
                )
            else:
                raise ValueError("No valid centroids calculated")

        except Exception as e:
            logging.error(f"Error in reduction: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return mapreduce_pb2.ReducerResponse(success=False, message=str(e), task_id=request.task_id)


async def serve():
    server = grpc.aio.server()
    mapreduce_pb2_grpc.add_ReducerServiceServicer_to_server(Reducer(), server)
    server.add_insecure_port('[::]:50054')
    await server.start()
    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        await server.stop(None)

if __name__ == '__main__':
    asyncio.run(serve())
