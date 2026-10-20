import threading
import zmq
import uuid
import time

class GroupServer:
    def __init__(self):
        self.context = zmq.Context()
        self.register_socket = self.context.socket(zmq.REQ)
        self.receive_socket = self.context.socket(zmq.ROUTER)
        self.uuid = str(uuid.uuid1())
        self.USERTELE = []
        self.message_history = []

    def start(self, name, ip, port, message_server_ip, message_server_port):
        self.name = name
        self.ip = ip
        self.port = port
        self.message_server_ip = message_server_ip
        self.message_server_port = message_server_port

        self.register_socket.connect(f"tcp://{message_server_ip}:{message_server_port}")
        self.register()
        self.receive_socket.bind(f"tcp://*:{self.port}")
        print(f"Group Server {self.name} listening on tcp://*:{self.port}")

        count = 0
        while True:
            count += 1
            print(count)
            # Receive multipart message
            multipart_message = self.receive_socket.recv_multipart()
            identity = multipart_message[0]
            action = multipart_message[1].decode("utf-8")
            message = multipart_message[2].decode("utf-8")
            
            print(message)
            full_message = f"{action} {message}"
            action_parts = full_message.split()

            print (action_parts[0])
            if action_parts[0] == "join_group":
                threading.Thread(target=self.join_user, args=(identity,action_parts[1])).start()
               
            elif action_parts[0] == "leave_group":
                threading.Thread(target=self.leave_user, args=(identity, action_parts[1])).start()
                # self.leave_user(message)
            elif action_parts[0] == "send_message":
                # timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                threading.Thread(target=self.store_message, args=(identity, action_parts[1], action_parts[2],action_parts[3])).start()
                # self.store_message(message, timestamp)
                
            elif action_parts[0] == "fetch_messages":
                timestamp = None
                if len(multipart_message) > 3:
                    timestamp = multipart_message[3].decode("utf-8")
                # self.fetch_messages(message, timestamp)
                if timestamp :
                    threading.Thread(target=self.fetch_messages, args=(identity, action_parts[1],action_parts[2])).start()
                else:
                    threading.Thread(target=self.fetch_messages, args=(identity, action_parts[1])).start()
                
                
    # def register(self):
    #     self.register_socket.send_multipart([b"register", self.name.encode("utf-8"), self.ip.encode("utf-8"), str(self.port).encode("utf-8")])
    #     response = self.register_socket.recv_string()
    #     if response == "SUCCESS":
    #         print("Group server registered successfully.")
    def register(self):
        self.register_socket.send_json({"action": "register", "name": self.name, "ip": self.ip, "port": self.port})
        response = self.register_socket.recv_string()
        if response == "SUCCESS":
            print("Group server registered successfully.")

    def join_user(self, identity, user_uuid):
        print(f"JOIN REQUEST FROM {user_uuid}")
        self.USERTELE.append(user_uuid)
        print("User joined successfully.")

        self.receive_socket.send_multipart([identity, b"", b"SUCCESS"])
        # self.receive_socket.send_string("SUCCESS")
        return 0

    def leave_user(self, identity,user_uuid):
        print(f"LEAVE REQUEST FROM {user_uuid}")
        self.USERTELE.remove(user_uuid)
        print("User left group successfully.")
        self.receive_socket.send_multipart([identity, b"", b"SUCCESS"])
        # self.receive_socket.send_string("SUCCESS")

    def store_message(self,identity, user_uuid, message, timestamp):
        message_data = {"user_uuid": user_uuid, "message": message, "timestamp": timestamp}
        self.message_history.append(message_data)
        print("Message history" + str(self.message_history))
        print("Message sent in group.")
        self.receive_socket.send_multipart([identity, b"", b"SUCCESS"])
        # self.receive_socket.send_string("SUCCESS")

    def fetch_messages(self, identity, user_uuid, timestamp=None):
        if user_uuid in self.USERTELE:
            if timestamp:
                filtered_messages = [msg for msg in self.message_history if msg["timestamp"] >= timestamp]
                self.receive_socket.send_multipart([identity, b"", str(filtered_messages).encode("utf-8")])
                return filtered_messages
            else:
                self.receive_socket.send_multipart([identity, b"", str(self.message_history).encode("utf-8")] + [str(self.message_history).encode("utf-8")])
                return self.message_history
        else:
            self.receive_socket.send_multipart([identity, b"", b"FAIL"])
            return None


if __name__ == "__main__":
    group = GroupServer()
    group.start("Group1", "10.128.0.7", 1234, "10.128.0.6", 2000)
