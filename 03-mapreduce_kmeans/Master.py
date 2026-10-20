import asyncio
import grpc.aio
import mapreduce_pb2
import mapreduce_pb2_grpc
import logging
import numpy as np
import pandas as pd
import os
import sys

# ports for each mapper
MAPPERS = {
    "mapper1": "50051",
    "mapper2": "50052",
    "mapper3": "50053",
}

mapper_ports = ["mapper1", "mapper2", "mapper3"]

REDUCERS = {
    "reducer1": "50054",
    "reducer2": "50055",
    "reducer3": "50056"
}

data_path = 'Data/Input/points.txt'

def get_input_parameters():
    """
    Prompts the user to input necessary configuration parameters.
    """
    num_mappers = int(input("Enter the number of mappers: "))
    num_reducers = int(input("Enter the number of reducers: "))
    num_centroids = int(input("Enter the number of centroids: "))
    max_iterations = int(input("Enter the maximum number of K-Means iterations: "))
    return num_mappers, num_reducers, num_centroids, max_iterations


# Configure logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    handlers=[logging.FileHandler("dump.txt"),
                              logging.StreamHandler()])

def random_initial_centroids(data_path, k):
    """
    Randomly selects 'k' centroids from a given dataset.
    """
    try:
        data = pd.read_csv(data_path, header=None)
        centroids = data.sample(n=k).to_numpy()
        return centroids
    except Exception as e:
        logging.error(f"Error reading the data or selecting centroids: {str(e)}")
        sys.exit("Failed to initialize centroids.")


def split_data(num_mappers, data_path):
    """
    Splits the data indices and assigns them to mappers.
    """
    data = pd.read_csv(data_path, header=None)
    total_points = len(data)
    chunk_size = total_points // num_mappers
    data_indices = {}
    for i in range(num_mappers):
        start = i * chunk_size
        end = (i + 1) * chunk_size if i < num_mappers - 1 else total_points
        data_indices[f"mapper{i+1}"] = (start, end)
    return data_indices


# Helper function to get a gRPC stub for a given mapper
async def get_mapper_stub(port):
    channel = grpc.aio.insecure_channel(f'localhost:{port}')
    await channel.channel_ready()
    logging.info(f"Connected to mapper at port {port}")
    return mapreduce_pb2_grpc.MapperServiceStub(channel)


async def get_reducer_stubs(num_reducers):
    reducer_stubs = {}
    for i in range(1, num_reducers + 1):
        port = REDUCERS[f"reducer{i}"]
        channel = grpc.aio.insecure_channel(f'localhost:{port}')
        await channel.channel_ready()
        reducer_stubs[f"reducer{i}"] = mapreduce_pb2_grpc.ReducerServiceStub(channel)
    return reducer_stubs

def serialize_centroids(centroids):
    """Serialize centroids to ensure they are a flat list of floats."""
    if isinstance(centroids, np.ndarray):
        return centroids.flatten().tolist()  # Convert numpy array to flat list
    elif isinstance(centroids, list):
        # Ensure each element is float
        return [float(coord) for coord in centroids]
    raise ValueError("Centroids must be a list or numpy array")

def save_compiled_centroids(centroids):
    np.savetxt('Data/centroids.txt', np.array(centroids), delimiter=",", fmt="%f")
    logging.info("Compiled centroids are saved in Data/centroids.txt")


async def send_task_to_mapper(mapper_name, stub, centroids, task_id, num_reducers, indices):
    try:
        logging.debug(f"Sending to {mapper_name}: centroids={centroids}, task_id={task_id}, num_reducers={num_reducers}")


        flat_centroids = serialize_centroids(centroids)
        logging.debug(f"Sending to {mapper_name}: centroids={flat_centroids}, indices={indices}, task_id={task_id}, num_reducers={num_reducers}")

        response = await stub.ProcessInput(
            mapreduce_pb2.MapperTask(
                centroids=flat_centroids,
                input_data_indices=indices,  # Ensuring indices are sent correctly
                task_id=str(task_id),
                num_reducers=int(num_reducers)
            )
        )
        logging.info(f"Response from {mapper_name}: {response.message} for Task {task_id}")
        return response
    except grpc.aio.AioRpcError as e:
        logging.error(f"Failed to send task to {mapper_name}: {str(e)}")
        return None



async def send_task_to_reducers(reducer_stubs, data_paths, task_id, num_reducers):
    tasks = []
    for reducer_name, stub in reducer_stubs.items():
        task = stub.ProcessIntermediateData(
            mapreduce_pb2.ReducerTask(
                data_paths=data_paths,
                task_id=task_id,
                num_reducers=num_reducers
            )
        )
        tasks.append(task)

    compiled_centroids = []
    try:
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        for response in responses:
            if isinstance(response, Exception):
                logging.error(f"Reducer task failed with exception: {str(response)}")
                continue
            if response.success:
                logging.info(f"Reducer {reducer_name} processed data successfully with task ID {task_id}")
                # Assume that reducer responses now include the centroid data
                compiled_centroids.extend(response.centroids)
            else:
                logging.error(f"Reducer {reducer_name} failed to process data with task ID {task_id}: {response.message}")
        if not compiled_centroids:
            logging.error("No successful reducer responses were received.")
        return compiled_centroids
    except Exception as e:
        logging.error(f"Failed to send task to reducers with task ID {task_id}: {str(e)}")
        return []


async def wait_for_mappers(mapper_stubs):
    """Wait for all mappers to complete their tasks by checking if they are not running."""
    # Continuously check if all mappers are done
    while True:
        tasks = [stub.GetStatus(mapreduce_pb2.StatusRequest()) for stub in mapper_stubs.values()]
        responses = await asyncio.gather(*tasks)
        # Check if all mappers are not running
        if all(not response.is_running for response in responses):
            break  # Exit the loop if all mappers are done
        await asyncio.sleep(1)  # Wait a bit before checking again

    return responses

def read_centroids_from_file(file_path):
    try:
        return np.loadtxt(file_path, delimiter=',')
    except Exception as e:
        logging.error(f"Failed to read centroids from {file_path}: {str(e)}")
        return None



def has_converged(old_centroids, new_centroids, tolerance=0.0001):
    try:
        # Handle potential string paths to centroid files
        if isinstance(new_centroids, str):
            new_centroids = np.loadtxt(new_centroids, delimiter=",")
            logging.debug(f"Loaded new centroids from file: {new_centroids}")

        if isinstance(old_centroids, str):
            old_centroids = np.loadtxt(old_centroids, delimiter=",")
            logging.debug(f"Loaded old centroids from file: {old_centroids}")

        # Ensure both are numpy arrays
        old_centroids = np.array(old_centroids, dtype=float)
        new_centroids = np.array(new_centroids, dtype=float)

        # Calculate total movement
        total_movement = sum(np.linalg.norm(oc - nc) for oc, nc in zip(old_centroids, new_centroids))
        logging.debug(f"Total movement: {total_movement}, Tolerance: {tolerance}")

        # Convergence check
        has_converged = total_movement < tolerance
        logging.debug(f"Convergence check: {'passed' if has_converged else 'failed'}")
        return has_converged
    except Exception as e:
        logging.error("Error checking convergence:", exc_info=True)
        return False



async def run_master(num_mappers, num_reducers, num_centroids, max_iterations, data_path):
    initial_centroids = random_initial_centroids(data_path, num_centroids)
    if initial_centroids.size == 0:
        logging.error("Centroids initialization failed. Exiting.")
        return
    
    logging.info(f"Randomly initialized centroids: {initial_centroids}")
    mapper_stubs = {f"mapper{i+1}": await get_mapper_stub(MAPPERS[f"mapper{i+1}"]) for i in range(num_mappers)}
    reducer_stubs = await get_reducer_stubs(num_reducers)
    current_centroids = initial_centroids

    for iteration in range(max_iterations):
        logging.info(f"Starting iteration {iteration + 1}")
        logging.debug(f"Current centroids before sending to mappers: {current_centroids}")
        tasks = []
        for i in range(num_mappers):
            mapper_id = f"mapper{i+1}"
            task_id = f"{mapper_id}_{iteration}"
            stub = mapper_stubs[mapper_id]
            flat_centroids = serialize_centroids(current_centroids)
            data_indices = split_data(num_mappers, data_path)[mapper_id]  # Ensure the key is correctly used
            task = send_task_to_mapper(mapper_id, stub, flat_centroids, task_id, num_reducers, data_indices)
            tasks.append(task)
        
        mapper_responses = await asyncio.gather(*tasks)
        if all(response and response.success for response in mapper_responses):
            data_paths = [response.intermediate_data_path for response in mapper_responses if response]
            new_centroids = await send_task_to_reducers(reducer_stubs, data_paths, task_id, num_reducers)
            if new_centroids:
                save_compiled_centroids(new_centroids)
                logging.debug(f"New centroids loaded from reducer output: {new_centroids}")
                if has_converged(current_centroids, new_centroids):
                    logging.info("Convergence has been reached.")
                    current_centroids = new_centroids
                    break
                else:
                    logging.info("Updating centroids for the next iteration.")
                    current_centroids = new_centroids
            else:
                logging.error("No new centroids were obtained from reducers or they were invalid.")
        else:
            logging.error("One or more mappers failed, aborting this iteration.")
            break

    logging.info(f"Final centroids: {current_centroids}")


async def main():
    num_mappers, num_reducers, num_centroids, max_iterations = get_input_parameters()
    await run_master(num_mappers, num_reducers, num_centroids, max_iterations, data_path)

if __name__ == '__main__':
    asyncio.run(main())