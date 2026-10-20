import os
import json
import time
import grpc.aio as grpc
import random
import asyncio
import raftkv_pb2
import raftkv_pb2_grpc
from concurrent import futures

node_addresses = {
    '01': 'localhost:50051',
    '02': 'localhost:50052',
    '03': 'localhost:50053',
}

class RaftNode:
    def __init__(self, nodeID, node_addresses):
        self.node_id = nodeID
        self.acks = {}                    # dictionary to save the last index of every node
        for node_id, address in node_addresses.items():
            self.acks[node_id] = 0
        self.node_addresses = node_addresses
        self.state = "follower"           # follower, candidate, leader
        self.current_term = 0
        self.voted_for = None
        self.commit_log = []
        self.log = []                   # List of log entries; each entry is a dictionary
        self.data = {}
        self.previous_commit = -1
        self.commit_index = -1
        self.election_timeout = random.uniform(5, 10)
        self.heartbeat_interval = 1.0       # 1 second
        self.heartbeat_received = False
        self.lease_with_others = 0
        self.lease_expiration_time = None 
        self.lease_duration = 5.0           # 5 second
        self.leader_id = None
        self.lock = asyncio.Lock()
        self.initialize_storage()

    def initialize_storage(self):
        # Load or initialize persistent state (term, votedFor, log)
        directory = f"./logs/logs_node_{self.node_id}"
        if not os.path.exists(directory):
            os.makedirs(directory)
        self.log_file_path = os.path.join(directory, "logs.txt")
        self.metadata_file_path = os.path.join(directory, "metadata.txt")
        self.dump_file_path = os.path.join(directory, "dump.txt")
        self.data_file_path = os.path.join(directory, "data.txt")
        self.committed_log_path = os.path.join(directory, "committed.txt")
        # Try to load the log from disk
        try:
            with open(self.log_file_path, 'r') as log_file:
                self.log = json.load(log_file)
        except FileNotFoundError:
            self.log = []  # Initialize log as empty if file does not exist
        
        # Try to load the committed log from disk
        try:
            with open(self.committed_log_path, 'r') as commit_file:
                self.commit_log = json.load(commit_file)
        except FileNotFoundError:
            self.commit_log = []  # Initialize log as empty if file does not exist
        
        # Try to load metadata from disk
        try:
            with open(self.metadata_file_path, 'r') as metadata_file:
                metadata = json.load(metadata_file)
                self.current_term = metadata['current_term']
                self.voted_for = metadata.get('voted_for')  # Might be None if not set
                self.commit_index = metadata.get('commitLength')
                self.acks = metadata.get('acks')
        except FileNotFoundError:
            # Initialize metadata if file does not exist
            self.current_term = 0
            self.voted_for = None
            self.save_metadata()

        # Try loading the data
        try:
            with open(self.data_file_path, 'r') as f:
                self.data = json.load(f)
        except FileNotFoundError:
            self.data = {}       
        temp_text = f"Node {self.node_id} started"
        self.dump_operation(temp_text)
        print(temp_text)
        self.save_metadata()

    def dump_operation(self, line):
        try:
            with open(self.dump_file_path, 'a') as f:
                # Ensure each line ends with a newline character
                if not line.endswith('\n'):
                    line += '\n'
                f.write(line)
        except:
            print("dumping operation ran into some error")

    def save_metadata(self):
        # Save current term and voted_for to disk
        metadata = {
            'commitLength': self.commit_index,
            'current_term': self.current_term,
            'voted_for': self.voted_for,
            'acks': self.acks
        }
        try:
            with open(self.metadata_file_path, 'w') as metadata_file:
                json.dump(metadata, metadata_file)
        except:
            print("save_metadata ran into an issue")

    async def save_state(self):
        try:
            with open(self.data_file_path, 'w') as data_file:
                json.dump(self.data, data_file)
        except:
            print("Data save unsuccessful")
        try:
            with open(self.committed_log_path, 'w') as commit_file:
                json.dump(self.commit_log, commit_file)
        except:
            print("commit_log save unsuccessful")
        try:
            with open(self.log_file_path, 'w') as log_file:
                json.dump(self.log, log_file)
        except:
            print("Logs save unsuccessful")
        try:
            self.save_metadata()
        except:
            print("Metadata save unsuccessful")
    
    def get_last_log_index(self):
        return len(self.log) - 1 if self.log else -1

    def get_last_log_term(self):
        return self.log[-1]['term'] if self.log else 0

    def calculate_lease_duration_remaining(self):
        if not self.lease_expiration_time:
            return 0
        now = time.time()
        return max(0, self.lease_expiration_time - now)
        
    async def run(self):
        # Start the election timer
        print(f"Node {self.node_id} started, election timer started with {self.election_timeout} seconds")
        asyncio.create_task(self.election_timer())
        
        # Additional logic for starting the server, handling RPCs, etc.

    async def become_leader(self):
        self.state = 'leader'  # Become the leader
        self.leader_id = self.node_id
        self.lease_expiration_time = time.time() + self.lease_duration
        entry = {"term": self.current_term, "Operation": "NO-OP", "key": '', "value": ''}
        self.log.append(entry)
        temp_text = f"Node {self.node_id} became the leader for term {self.current_term}"
        print(temp_text)
        self.dump_operation(temp_text)
        asyncio.create_task(self.lease_timer())
        # Start sending heartbeats
        asyncio.create_task(self.send_heartbeats())
    
    async def lease_timer(self):
        if self.state == 'leader' and self.lease_with_others:
            temp_text = "New Leader waiting for Old Leader Lease to timeout."
            print(temp_text)
            self.dump_operation(temp_text)
            await asyncio.sleep(self.lease_with_others)
        
        self.lease_with_others = 0
        await asyncio.sleep(2*self.heartbeat_interval)

        if self.state == 'leader' and not self.lease_with_others:
            while self.state == 'leader' and self.lease_expiration_time and time.time() < self.lease_expiration_time:
                await asyncio.sleep(0.5)  # Check lease status every 0.5 seconds
            if self.state == 'leader' and time.time() > self.lease_expiration_time:  # If still the leader but lease expired
                temp_text = f"Leader {self.node_id} lease renewal failed. Stepping Down."
                self.dump_operation(temp_text)
                print(temp_text)
                self.state = 'follower'  # Become a follower
                self.voted_for = None
                self.leader_id = None
    
    async def election_timer(self):
        while True:
            await asyncio.sleep(self.election_timeout)
            if not self.heartbeat_received and self.state != 'leader':
                dump_text = f"Node {self.node_id} election timer timed out, starting election"
                print(dump_text)
                self.dump_operation(dump_text)
                await self.start_election()
            self.heartbeat_received = False  # Reset 
    
    async def start_election(self):
        if self.state == 'leader':
            return
        self.state = 'candidate'
        self.current_term += 1
        self.voted_for = self.node_id
        self.heartbeat_received = False  # Reset heartbeat received flag
        max_lease = 0
        print(f"Node {self.node_id} starting an election for term {self.current_term}.")

        votes_received = 1  # Vote for itself
        # Async method to send out RequestVote RPCs to all nodes and await responses
        for node_id, address in node_addresses.items():
            if node_id != self.node_id:
                try:
                    response = await self.request_vote(node_id, address)
                    if response and response.voteGranted:
                        votes_received += 1
                        max_lease = max(max_lease, response.leaseDurationRemaining)
                    elif response and response.term > self.current_term:
                        temp_text = f"{self.node_id} Stepping down"
                        self.state = 'follower'
                        self.voted_for = None
                        self.leader_id = None
                        votes_received = 0
                        break 
                except grpc.AioRpcError as e:
                    temp_text = f"Error occurred while sending RPC to Node {node_id}."
                    print(temp_text)
                    self.dump_operation(temp_text)

        if votes_received > len(node_addresses) // 2:
            temp_text = f"Node {self.node_id} became the leader for term {self.current_term}."
            print(temp_text)
            self.dump_operation(temp_text)
            self.lease_with_others = max_lease
            await self.become_leader()

    def apply_committed_entries(self):
        """Applies committed log entries to the node's state machine."""
        # Ensure there's a lock to prevent concurrent writes
        if self.previous_commit < self.commit_index:
            for entry in self.commit_log:
                if entry['Operation'] == "SET":
                    if self.state == 'leader':
                        temp_text = f"Node {self.node_id} (leader) committed the entry SET {entry['key']} = {entry['value']} to the state machine."
                    else:
                        temp_text = f"Node {self.node_id} (follower) committed the entry SET {entry['key']} = {entry['value']} to the state machine."

                    self.dump_operation(temp_text)
                    print(temp_text)
                    self.data[entry['key']] = entry['value']
                    # Logging for debugging
                    print(f"Applied operation: SET {entry['key']} = {entry['value']}")
            self.previous_commit = self.commit_index
    
    async def commit_entries(self):
        if self.state == 'leader': # and self.previous_commit < self.commit_index:
            # Ensure there are entries to commit
            if not self.log:
                return
            
            # Step 1: Calculate the minimum next_index across all followers
            # Subtract 1 because next_index points to the next expected entry
            self.acks[self.node_id] = max(self.acks.values())
            min_next_index = min(self.acks.values(), default=self.commit_index+1) - 1

            # Step 2: Determine the highest log index that can be safely committed
            # (i.e., replicated on a majority of nodes)
            indexes_replicated_majority = [index for index in range(self.commit_index, min_next_index + 1) if self.is_replicated_on_majority(index)]

            if indexes_replicated_majority:
                # Step 3: Update the commit index to the highest index that meets the criteria
                self.commit_index = max(indexes_replicated_majority)
                # Now that commit_index is updated, apply the entries to the state machine and
                # persist the updated commit_index
                self.commit_log = self.log[:self.commit_index + 1]
                self.apply_committed_entries()
                await self.save_state()
        
        else: # self.previous_commit < self.commit_index:
            # Ensure there are entries to commit
            if not self.log:
                return
            self.commit_log = self.log[:self.commit_index + 1]
            self.apply_committed_entries()
            await self.save_state()
            

    def is_replicated_on_majority(self, log_index):
        # Count the number of nodes that have replicated the entry at log_index
        count_replicated = sum(ack >= log_index + 1 for ack in self.acks.values())  # +1 because acks store next_index
        # Include the leader in the count
        count_replicated += 1
        # Check if the count constitutes a majority
        return count_replicated > len(node_addresses) // 2
            
    async def send_heartbeats(self):
        while self.state == 'leader' and not self.lease_with_others and self.lease_expiration_time:
            if time.time() >= self.lease_expiration_time:
                print(f"Leader {self.node_id} lease renewal failed. Stepping Down.")
                self.state = 'follower'
                self.voted_for = None
                self.leader_id = None
                break  # Exit the loop if the lease has expired

            acks_received = 1

            for node_id, address in self.node_addresses.items():
                if node_id == self.node_id:
                    continue  # Skip sending heartbeat to self

                async with grpc.insecure_channel(address) as channel:
                    stub = raftkv_pb2_grpc.RaftStub(channel)
                    next_index = self.acks[node_id] if self.acks[node_id] <= len(self.log) - 1 else len(self.log) - 1
                    prev_log_index = next_index - 1
                    prev_log_term = self.log[prev_log_index]['term'] if prev_log_index >= 0 else 0
                    entries = [raftkv_pb2.LogEntry(term=e['term'], key=e['key'], value=e['value']) for e in self.log[next_index:]]
                    if not self.lease_expiration_time or time.time() + 3*self.heartbeat_interval >= self.lease_expiration_time:
                        lease_renew = int(self.lease_duration)
                    else:
                        lease_renew = 0

                    request = raftkv_pb2.AppendEntriesRequest(
                        term=self.current_term,
                        leaderId=self.node_id,
                        prevLogIndex=prev_log_index,
                        prevLogTerm=prev_log_term,
                        entries=entries,  # Convert log entries to the required format if necessary
                        leaderCommit=self.commit_index,
                        leaseDuration=lease_renew
                    )

                    try:
                        response = await stub.AppendEntries(request, timeout=5)
                        if response and response.success:
                            # If successful, update next_index for the follower
                            acks_received += 1
                            self.acks[node_id] = prev_log_index + len(entries) + 1
                            self.acks[self.node_id] = max(self.acks.values())
                            print(f"AppendEntries success to {node_id}")
                        elif response and response.term > self.current_term:
                            # Handle the case where the follower's term is higher
                            self.current_term = request.term
                            self.voted_for = None
                            self.state = "follower"
                        else:
                            # On failure, decrement next_index for the follower and retry later
                            self.acks[node_id] = max(0, self.acks[node_id] - 1)
                            print(f"AppendEntries failure to {node_id}, decrementing next_index")
                    except Exception as e:
                        temp_text = f"Error occurred while sending RPC/heartbeat to Node {node_id}."
                        self.dump_operation(temp_text)
                        print(temp_text)

            if acks_received > len(node_addresses) // 2:
                await self.commit_entries()
                # Optionally, renew lease if close to expiration
                if self.lease_expiration_time and time.time() + 2*self.heartbeat_interval >= self.lease_expiration_time:
                    self.lease_expiration_time = time.time() + self.lease_duration
                    print("Lease renewed.")
                    temp_text = f"Leader {self.node_id} sending heartbeat & Renewing Lease"
                    self.dump_operation(temp_text)
            elif acks_received < len(node_addresses) // 2 and time.time() > self.lease_expiration_time:
                temp_text = f"Leader {self.node_id} lease renewal failed. Stepping Down."
                self.dump_operation(temp_text)
                print(temp_text)

            # Sleep to ensure heartbeats are sent at regular intervals
            await asyncio.sleep(self.heartbeat_interval)
    
    async def AppendEntries(self, request, context):
        # Step 1: Verify the Leader's Term
        if request.term < self.current_term:
            temp_text = f"Node {self.node_id} rejected AppendEntries RPC from {request.leaderId}."
            self.dump_operation(temp_text)
            print(temp_text)
            return raftkv_pb2.AppendEntriesResponse(term=self.current_term, success=False)
        
        self.heartbeat_received = True
        self.leader_id = request.leaderId
        self.lease_with_others = time.time() + self.lease_duration

        # Step 2 & 3: Log Matching and Conflicting Entries
        if request.prevLogIndex > self.get_last_log_index():
            temp_text = f"Node {self.node_id} rejected AppendEntries RPC from {request.leaderId}."
            self.dump_operation(temp_text)
            print(temp_text)
            return raftkv_pb2.AppendEntriesResponse(term=self.current_term, success=False)
        
        # Find the first index in new entries that doesn't match the follower's log
        index = 0
        for i, entry in enumerate(request.entries):
            log_index = request.prevLogIndex + 1 + i
            if log_index < len(self.log) and self.log[log_index]['term'] != entry.term:
                index = i
                break
            elif log_index >= len(self.log):
                index = i
                break
        
        # Step 4: Append Any New Entries
        if index < len(request.entries):
            self.log = self.log[:request.prevLogIndex + 1 + index] + [
                {
                    "term": e.term,
                    "key": e.key,
                    "value": e.value,
                    "Operation": "NO-OP" if e.key == "" else "SET"
                } for e in request.entries[index:]
            ]

        
        # Step 5: Update Commit Index
        if request.leaderCommit > self.commit_index:
            self.commit_index = min(request.leaderCommit, len(self.log) - 1)
            # Apply the committed entries to the state machine and persist them
            await self.commit_entries()
        
        temp_text = f"Node {self.node_id} accepted AppendEntries RPC from {self.leader_id}."
        self.dump_operation(temp_text)
        print(temp_text)
        self.save_metadata()
        await self.save_state()

        return raftkv_pb2.AppendEntriesResponse(term=self.current_term, success=True)

    
    def RequestVote(self, request, context):
        self.heartbeat_received = True
        # Calculate the remaining lease duration
        lease_duration_remaining = self.calculate_lease_duration_remaining()

        if request.term < self.current_term:
            temp_text = f"Vote denied  for Node {request.candidateId} in term {request.term}."
            self.dump_operation(temp_text)
            print(temp_text)
            return raftkv_pb2.RequestVoteResponse(term=self.current_term, voteGranted=False, leaseDurationRemaining=lease_duration_remaining)

        # Update current term and reset votedFor if term is outdated
        if request.term > self.current_term:
            self.current_term = request.term
            self.voted_for = None
            self.state = "follower"

        if (self.voted_for is None or self.voted_for == request.candidateId) and self.is_log_up_to_date(request.lastLogIndex, request.lastLogTerm):
            temp_text = f"Vote granted for Node {request.candidateId} in term {request.term}."
            self.dump_operation(temp_text)
            print(temp_text)
            self.voted_for = request.candidateId
            self.save_metadata()
            return raftkv_pb2.RequestVoteResponse(term=self.current_term, voteGranted=True, leaseDurationRemaining=lease_duration_remaining)
        else:
            temp_text = f"Vote denied  for Node {request.candidateId} in term {request.term}."
            self.dump_operation(temp_text)
            print(temp_text)
            return raftkv_pb2.RequestVoteResponse(term=self.current_term, voteGranted=False, leaseDurationRemaining=lease_duration_remaining)
    
    async def request_vote(self, node_id, address):
        if self.state == 'candidate':
            async with grpc.insecure_channel(address) as channel:
                stub = raftkv_pb2_grpc.RaftStub(channel)
                # Construct the RequestVoteRequest message
                request = raftkv_pb2.RequestVoteRequest(
                    term=self.current_term,
                    candidateId=self.node_id,
                    lastLogIndex=self.get_last_log_index(),
                    lastLogTerm=self.get_last_log_term(),
                    leaseDurationRemaining=self.calculate_lease_duration_remaining()
                )
                try:
                    # Send the request and await the response
                    response = await stub.RequestVote(request, timeout=5)  # Consider a suitable timeout
                    return response
                except grpc.AioRpcError as e:
                    temp_text = f"Error occurred while sending RPC to Node {node_id}."
                    print(temp_text)
                    self.dump_operation(temp_text)
                    return None
        else:
            print("Not a candidate, back to follower")
            self.state = 'follower'
            self.voted_for = None
            self.leader_id = None
            return None

    
    def is_log_up_to_date(self, candidate_last_log_index, candidate_last_log_term):
        # If the log is empty, treat the term and index as 0
        last_log_index = self.get_last_log_index()
        last_log_term = self.get_last_log_term()
        
        # First, compare the term of the last log entry
        if last_log_term < candidate_last_log_term:
            return True
        elif last_log_term > candidate_last_log_term:
            return False
        
        # If the terms are equal, compare the indices of the last log entry
        return last_log_index <= candidate_last_log_index


    async def ServeClient(self, request, context):
        if self.state != 'leader':  
            leader_address = self.node_addresses.get(self.leader_id, "")
            return raftkv_pb2.ServeClientReply(success=False, leaderID=self.leader_id, data=leader_address)
        
        # Handle the request if this node is the leader
        parts = request.request.split(maxsplit=2)
        operation = parts[0]
        key = parts[1] if len(parts) > 1 else None
        value = parts[2] if len(parts) > 2 else None

        temp_text = f"Node {self.node_id} (leader) received an {operation} {key} {value} request."
        self.dump_operation(temp_text)
        print(temp_text)
        
        if operation.upper() == "SET":
            # Logic for handling SET operation
            try:
                result = await self.handle_set_operation(key, value)
            except:
                print("Ran into an issue in ServeClient")

            return raftkv_pb2.ServeClientReply(success=result["success"], leaderID=self.node_id, data=result["message"])
        elif operation.upper() == "GET":
            # Logic for handling GET operation
            value = await self.handle_get_operation(key)
            print("Client GET request response: ", key, ": ", value)
            return raftkv_pb2.ServeClientReply(success=True, leaderID=self.node_id, data=value)
        else:
            return raftkv_pb2.ServeClientReply(success=False, leaderID=self.node_id, data="Invalid operation")

    async def handle_set_operation(self, key, value):
        if self.state == 'leader' and not self.lease_with_others and self.lease_expiration_time and time.time() < self.lease_expiration_time:
            # Append the new operation to the log
            new_log_index = len(self.log)  # Get the index where the new entry will be added
            entry = {"term": self.current_term, "Operation": "SET", "key": key, "value": value}
            self.log.append(entry)
            
            # Wait for the operation to be committed
            success = await self.wait_for_commit(new_log_index, timeout=2)  # Example 5-second timeout
            print(f"Client SET Request: SET {key} {value} appended")
            
            if success and self.commit_index >= new_log_index:
                return {"success": True, "message": "SET operation successful"}
            else:
                return {"success": False, "message": "SET operation failed"}
        else:
            print("Not the leader or lease expired; cannot handle SET operation.")
            return {"success": False, "message": "Not the leader or lease expired"}

    async def wait_for_commit(self, log_index, timeout):
        """Waits for a log entry at a given index to be committed."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.commit_index >= log_index:
                return True  # The operation has been committed
            await asyncio.sleep(0.01)  # Wait a bit before checking again
        return False  # Timeout expired before the operation was committed


    async def handle_get_operation(self, key):
        if self.state == 'leader' and not self.lease_with_others and self.lease_expiration_time and time.time() < self.lease_expiration_time:
            # Only the leader with a valid lease can handle GET operations with guaranteed consistency
            return self.data.get(key, "")
        else:
            # Follower nodes handling or leader without a valid lease
            # Depending on consistency requirements, consider redirecting to leader or serving stale data
            return "Redirect to leader or operation not allowed"

    
async def serve(node_id, node_addresses):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_node = RaftNode(node_id, node_addresses)
    raftkv_pb2_grpc.add_RaftServicer_to_server(raft_node, server)
    raftkv_pb2_grpc.add_KeyValueStoreServicer_to_server(raft_node, server)
    server.add_insecure_port(node_addresses[node_id])
    await server.start()
    try:
        await raft_node.run()  # Start the Raft node's tasks
        await server.wait_for_termination()
    except KeyboardInterrupt:
        # Attempt a graceful shutdown
        tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await server.stop(0)  # Await the server shutdown here
        print("Server and all tasks have been shut down gracefully.")

if __name__ == '__main__':
    # Example node_addresses format: {'node_1': 'localhost:50051', 'node_2': 'localhost:50052'}
    node_id = '02'
    asyncio.run(serve(node_id, node_addresses))