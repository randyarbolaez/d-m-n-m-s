import socket
import threading
import redis
from dotenv import load_dotenv
import os
import sys

class ChatServer:
    def __init__(self, initial_port):
        load_dotenv()
        self.port = int(initial_port)

        self.addresses_lookup = {}
        self.clients_lookup = {}
        self.usernames_lookup = {}

        self.redis_url = os.getenv("REDIS_URL")
        self.r = redis.Redis.from_url(self.redis_url)

        self.pubsub = self.r.pubsub()
        self.pubsub.subscribe("global_chat")
        self.pubsub.subscribe(f"node_{self.port}")

        try:
            self.s = socket.socket()
            print("Socket is successfully created!")
        except socket.error as e:
            print("Error creating socket: ", e)
            sys.exit(1)

        self.s.bind(('', int(self.port)))
        self.s.listen(5)

    def start(self):
        print(f"Server started and listening on port {self.port}")

        redis_thread = threading.Thread(target=self.get_redis_message, args=(), daemon= True)
        redis_thread.start()

        while True:
            c, address = self.s.accept()
            data = c.recv(1024).decode().split(" ")
            name = data[0]
            if not data or not name:
                c.close()
                continue

            existing_port = self.r.hget("usernames", name)
            if existing_port is None or int(existing_port.decode()) != self.port:
                self.addresses_lookup[address] = name
                self.clients_lookup[address] = c
                self.usernames_lookup[name] = address
                self.r.hset("usernames", name, self.port)

                current_port = self.r.hget("ports", self.port)
                if current_port is None:
                    self.r.hset("ports", self.port, 1)
                else:
                    self.r.hset("ports", self.port, int(current_port) + 1)

                if len(address) > 1:
                    self.send_message(address, " ".join(data).encode())

                client_thread = threading.Thread(target=self.get_message, args=(c,address, name), daemon= True)
                client_thread.start()
            else:
                c.sendall("Somebody already has that name. You'll be disconnected.".encode())
                c.shutdown(socket.SHUT_WR)
                c.close()

    def get_message(self, c, address, name):
        while True:
            data = c.recv(1024)

            if c._closed or not data:
                goodbye_message = f"{name} has left the chat"
                self.send_message(address, goodbye_message.encode())
                self.r.hdel("usernames", name)
                self.usernames_lookup.pop(name)
                self.addresses_lookup.pop(address)
                self.clients_lookup.pop(address)

                current_port = self.r.hget("ports", self.port)

                if current_port is not None:
                    new_count = int(current_port) - 1
                    if new_count <= 0:
                        self.r.hdel("ports", self.port)
                    else:
                        self.r.hset("ports", self.port, new_count)
                    c.close()
                    break

            decoded_data = data.decode()
            parts = decoded_data.split(" ")
            command = parts[0]
            if command == "/pm":
                if(len(parts) < 3):
                    c.sendall("WARNING: Invalid format. It's /pm <username> <message>".encode())
                    continue
                target_user = parts[1]
                msg = parts[2:]
                target_port = self.r.hget("usernames", target_user)
                if target_port is None:
                    c.sendall(f"WARNING: {target_user} doesn't exist.".encode())
                else:
                    self.send_private_message(name, target_user,  target_port, ' '.join(msg))
            else:
                self.send_message(address, data)
    
    def get_redis_message(self):
        while True:
            for message in self.pubsub.listen():
                if message['type'] == 'message':
                    channel = message['channel'].decode()
                    data_str = message['data'].decode()

                    if channel == f"node_{self.port}":
                        parts = data_str.split(":", 2)

                        if len(parts) >= 3:
                            from_user, to_user, chat_message = parts
                            target_address = self.usernames_lookup.get(to_user)

                            if target_address and target_address in self.clients_lookup:
                                self.clients_lookup[target_address].sendall(f"PRIVATE MESSAGE from {str(from_user)} : {chat_message}".encode())
                    else:
                        parts = data_str.split(":", 1)
                        if parts:
                            from_username = parts[0]

                            for client_address, client_socket in list(self.clients_lookup.items()):
                                local_username = self.addresses_lookup.get(client_address)
                                if local_username == from_username:
                                    continue
                                else:
                                    client_socket.sendall(message["data"])

    def send_message(self, from_address, msg):
        username = self.addresses_lookup.get(from_address, "Unknown")
        msg_str = msg.decode() if isinstance(msg, bytes) else msg
        actual_message = f"{username}:{msg_str}"
        self.r.publish('global_chat', actual_message)

    def send_private_message(self, from_user, to_user, to_port, msg):
        channel = f"node_{to_port.decode()}"
        self.r.publish(channel, f"{from_user}:{to_user}:{msg}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("python3 server.py <PORT>")
        sys.exit(1)

server = ChatServer(initial_port=sys.argv[1])
server.start()

## todo - bug - there are 2 servers, and 2 clients, server 1 = client 1, server 2 = client 2. if server 1 disconnects then client 1 will connect to server 2, but client 1 will not receive any of the messages that client 2 sends BUT client 1 can recieve messages from client 2.

## todo semaphore

## todo threading lock