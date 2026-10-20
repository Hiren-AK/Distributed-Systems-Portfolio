import zmq
import uuid
import time

class UserClient:
    def __init__(self):
        self.context = zmq.Context()
        self.message_socket = self.context.socket(zmq.REQ)
        self.group_socket = self.context.socket(zmq.REQ)
        self.uuid = str(uuid.uuid1())
        self.joined_groups = {}

    def start(self, message_server_ip, message_server_port):
        self.message_socket.connect(f"tcp://{message_server_ip}:{message_server_port}")

    # def request_live_groups(self):
    #     self.message_socket.send_string(f"get_groups {self.uuid}")
    #     group_list = self.message_socket.recv_string()
    #     print(group_list)
    def request_live_groups(self):
        self.message_socket.send_json({"action": "get_groups", "uuid": self.uuid})
        group_list = self.message_socket.recv_string()
        print(group_list)

    def join_group(self, group_ip, group_port):
        self.group_socket = self.context.socket(zmq.REQ)
        self.group_socket.connect(f"tcp://{group_ip}:{group_port}")
        print(f"Connected to group at {group_ip}:{group_port}")

        self.group_socket.send_string(f"join_group {self.uuid}")
        print("Join group request sent.")

        response = self.group_socket.recv_string()

        if response == "SUCCESS":
            self.joined_groups[group_port] = { "ip": group_ip, "port": group_port}
            print("SUCCESS")
        else:
            print("Failed to join the group.")

    def leave_group(self, group_ip, group_port):
        if not self.group_socket.closed:
            print("Closing existing socket.")
            self.group_socket.close()

        self.group_socket = self.context.socket(zmq.REQ)
        self.group_socket.connect(f"tcp://{group_ip}:{group_port}")
        print(f"Connected to group at {group_ip}:{group_port}")

        self.group_socket.send_string(f"leave_group {self.uuid}")
        print("leave group request sent.")

        response = self.group_socket.recv_string()

        if response == "SUCCESS":
            if name in self.joined_groups:
                del self.joined_groups[group_port]
            print("SUCCESS")
        else:
            print("Failed to leave the group.")

    def send_message(self, group_ip, group_port, message):

        self.group_socket = self.context.socket(zmq.REQ)
        self.group_socket.connect(f"tcp://{group_ip}:{group_port}")
        print(f"Connected to group at {group_ip}:{group_port}")

        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        self.group_socket.send_string(f"send_message {self.uuid} {message} {timestamp}")
        print("Message sent to group.")
        response = self.group_socket.recv_string()

        if response == "SUCCESS":
            print("SUCCESS")
        else:
            print("Failed to leave the group.")

    def fetch_messages(self, group_ip, group_port, timestamp=None):
        self.group_socket = self.context.socket(zmq.REQ)
        self.group_socket.connect(f"tcp://{group_ip}:{group_port}")
        print(f"Connected to group at {group_ip}:{group_port}")

        fetch_data = f"fetch_messages {self.uuid}"
        if timestamp:
            fetch_data += f" {timestamp}"

        self.group_socket.send_string(fetch_data)
        response = self.group_socket.recv_string()

        if response.startswith("Fail"):
            print("You are not part of this group.")

        else:
            print("Message History:")
            print(response)
            # messages = eval(response.split("SUCCESS "))
            # for message in messages:
            #     user_uuid, msg, timestamp = message.split(" ")
            #     print(f"User: {user_uuid}, Message: {msg}, Timestamp: {timestamp}")

    def display_menu(self):
        print("1. Request live groups")
        print("2. Join a group")
        print("3. Leave group")
        print("4. Send message in group")
        print("5. Fetch message from group")
        choice = input("Enter your choice: ")
        return choice


if __name__ == "__main__":
    user = UserClient()
    user.start("10.128.0.6", 2000)

    while True:
        choice = user.display_menu()

        if choice == '1':
            user.request_live_groups(
                
            )
        elif choice == '2':
            group_ip = input("Enter group IP: ")
            group_port = input("Enter group port: ")
            user.join_group(group_ip, group_port)
        elif choice == '3':
            print("Here is the list of all the groups you have joined :" + str(user.joined_groups))
            group_ip = input("Enter group IP: ")
            group_port = input("Enter group port: ")
            user.leave_group(group_ip, group_port)
        elif choice == '4':
            print("Here is the list of all the groups you have joined :" + str(user.joined_groups))
            group_ip = input("Enter group IP: ")
            group_port = input("Enter group port: ")
            message = input("Enter message: ")
            user.send_message(group_ip, group_port, message)
        elif choice == '5':
            print("Here is the list of all the groups you have joined :" + str(user.joined_groups))
            group_ip = input("Enter group IP: ")
            group_port = input("Enter group port: ")
            timestamp = input("Enter timestamp (optional - press enter to fetch all messages): ")
            user.fetch_messages(group_ip, group_port, timestamp)
        else:
            print("Invalid choice. Please select again.")



