import zmq

class MessageServer:
    def __init__(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.groups = {}

    def start(self, port):
        self.socket.bind(f"tcp://*:{port}")
        print("Message server is running.")

        while True:
            message = self.socket.recv_json()
            if message["action"] == "register":
                self.register_group(message["name"], message["ip"], message["port"])
            elif message["action"] == "get_groups":
                self.get_live_groups(message["uuid"])

    def register_group(self, name, ip, port):
        self.groups[name] = {"ip": ip, "port": port}
        print(f"JOIN REQUEST FROM {ip}:{port}")
        self.socket.send_string("SUCCESS")

    def get_live_groups(self, uuid):
        print(f"GROUP LIST REQUEST FROM {uuid}")
        group_list = "\n".join([f"{name} - {info['ip']}:{info['port']}" for name, info in self.groups.items()])
        self.socket.send_string(group_list)

if __name__ == "__main__":
    server = MessageServer()
    server.start(2000)
