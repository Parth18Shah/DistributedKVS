from flask import Flask, request, jsonify
import multiprocessing
import time
from werkzeug.serving import make_server
from enum import Enum
import threading
import socket
import struct
import random
import os

# Enum for node states
class Node_State(Enum):
    Leader = 1
    Candidate = 2
    Follower = 3

# Constants for multicast communication
MULTICAST_GROUP = "224.1.1.1"
PORT = 5007

class Node:
    def __init__(self, node_id, port, node_count, leader_id):
        self.data_store = {}
        self.node_id = node_id
        self.port = port
        self.process = None
        self.node_state = Node_State(3).name
        self.node_count = node_count
        self.majority_count = node_count//2 + 1 
        self.votes_received = 0 # Number of votes received for leader election
        self.running = True  # To stop threads gracefully
        self.last_leader_timestamp = time.time()  # Timestamp of the last leader election
        self.election_timeout = random.randint(10,30) # Random election timeout
        self.term = 0 # Current election term
        self.sock = None # Socket for multicast communication
        self.leader_id = leader_id
        self.log = [] # List of Log for set commands
        self.ack_rec = 0 # Number of acknowledgements received
    
    def start(self):
        print('Starting Node...')
        self.process = multiprocessing.Process(
            target=create_flask_app,
            args=(self.node_id, self.port)
        )
        self.process.start()
        print(f'Node {self.node_id} started with pid: {self.process.pid} on port: {self.port}')
        time.sleep(5)  # Give Flask time to initialize
    
    def join_multicast(self):
        # Start listening for multicast messages
        threading.Thread(target=self.listen_for_multicast, daemon=True).start()
    
    def stop(self):
        self.running = False
        if self.process and self.process.is_alive():
            self.process.terminate()
            self.process.join()
            print(f'Node {self.node_id} with PID {self.process.pid} stopped.')
    
    def listen_for_multicast(self):
        """
        Listens for incoming multicast messages and responds accordingly.
        """
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        if os.name == 'nt':
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # For Windows
        else:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.sock.bind(("", PORT))
        print(f"Node {self.node_id} Connected to Multicast socket")
        # Join multicast group
        mreq = struct.pack("4sl", socket.inet_aton(MULTICAST_GROUP), socket.INADDR_ANY)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

        while self.running:
            if self.term == 0:
                self.sock.settimeout(self.election_timeout)
            else:
                self.sock.settimeout(600 + random.randint(10,30))
            try:
                data, addr = self.sock.recvfrom(1024)
                message = data.decode()

                if message.startswith("RequestVote:"):
                    msg = message.split(":")
                    candidate_id = int(msg[1])
                    current_election_term = int(msg[2])
                    if self.node_state == Node_State(3).name and current_election_term > self.term:
                        self.send_vote(candidate_id, addr, current_election_term)

                elif message.startswith("LeaderElected:"):
                    leader_id = int(message.split(":")[1])
                    if leader_id != self.node_id:
                        print(f"Node {self.node_id} recognizes Leader {leader_id}")
                        self.node_state = Node_State(3).name
                        self.last_leader_timestamp = time.time()
                
                elif message.startswith("VoteGranted:"):
                    _, vote_for_node_id = map(int, message.split(":")[1:])
                    if self.node_id == vote_for_node_id and self.node_state == Node_State(2).name:
                        self.votes_received += 1
                        # print(f"Node {self.node_id} received a vote! Total: {self.votes_received}")
                        if self.votes_received >= self.majority_count:
                            print(f"Node {self.node_id} wins the election and becomes the leader!")
                            self.node_state = Node_State(1).name
                            self.leader_id[0] = self.node_id
                            self.announce_leadership(self.term)
                
                elif message.startswith("AppendSetCommandToLog:"):
                    key, value = message.split(":")[1:]
                    self.log.append((key, value))
                    self.ack_rec = 0
                    # print(f"Node {self.node_id} recevied the command: set key {key} to value {value}")
                    message = f"AckSetCommand:{key}:{value}:{self.node_id}".encode()
                    self.sock.sendto(message, (MULTICAST_GROUP, PORT))
                      
                elif message.startswith("AckSetCommand:"):
                    key, value, msg_from_node_id = message.split(":")[1:]
                    if self.node_id == self.leader_id[0]:
                        self.ack_rec += 1
                        # print(f"Node {self.node_id} received a ack from {msg_from_node_id}. Total: {self.ack_rec}")
                        if self.ack_rec >= self.majority_count:
                            self.data_store[key] = value
                            time.sleep(5)
                            if self.log:
                                message = f"SetCommand:{key}:{value}:{self.node_id}".encode()
                                self.sock.sendto(message, (MULTICAST_GROUP, PORT))
                            self.log = []
                
                elif message.startswith("SetCommand:"):
                    key, value, msg_from_node_id = message.split(":")[1:]
                    print(f"Node {self.node_id} with {self.leader_id} and state {self.node_state} received the command: set key {key} to value {value}")
                    if self.node_id != self.leader_id[0]:
                        self.data_store[key] = value
                        self.log = []

                elif message.startswith("GetCommand:"):
                    key = message.split(":")[1]
                    print(f"Node {self.node_id} with {self.leader_id} and state {self.node_state} received the command: get key {key}")
                    if self.node_id == self.leader_id[0]:
                        value = self.data_store.get(key, "Key not found")
                        time.sleep(5)
                        message = f"GetCommandResponse:{value}".encode()
                        self.sock.sendto(message, (MULTICAST_GROUP, PORT))

            except socket.timeout:
                if self.node_state == Node_State(1).name:
                    self.node_state = Node_State(3).name
                    self.votes_received = 0

                # No leader detected, check if election is needed
                elif time.time() - self.last_leader_timestamp > self.election_timeout:
                    print(f"Node {self.node_id} detected no leader. Starting election for term {self.term}...")
                    self.term += 1 
                    self.node_state = Node_State(2).name
                    self.start_election()
    
    def send_vote(self, candidate_id, addr, current_election_term):
        """
        Sends a unicast vote response to the candidate.
        """
        response = f"VoteGranted:{self.node_id}:{candidate_id}".encode()
        self.sock.sendto(response, (MULTICAST_GROUP, PORT))
        self.term = current_election_term

    def start_election(self):
        """
        Starts the leader election process.
        """
        self.votes_received = 1  # Vote for itself
        message = f"RequestVote:{self.node_id}:{self.term}".encode()
        self.sock.sendto(message, (MULTICAST_GROUP, PORT))
    
    def announce_leadership(self, term):
        """
        Announces that this node has become the leader.
        """
        message = f"LeaderElected:{self.node_id}:{term}".encode()
        self.sock.sendto(message, (MULTICAST_GROUP, PORT))

def create_flask_app(node_id, port):
    
    try:
        app = Flask(__name__)

        def __get_value(key):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
                    if os.name == 'nt':
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # For Windows
                    else:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                
                    # Join multicast group
                    group = struct.pack("4sl", socket.inet_aton(MULTICAST_GROUP), socket.INADDR_ANY)
                    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, group)
                    sock.bind(("", PORT))

                    message = f"GetCommand:{key}".encode()
                    sock.sendto(message, (MULTICAST_GROUP, PORT))
                    while True:
                        data, _ = sock.recvfrom(1024)
                        message = data.decode()
                        if message.startswith("GetCommandResponse:"):
                            value = message.split(":")[1]
                            if value == "Key not found": 
                                return jsonify({"error": "key not found"}), 400
                            return jsonify({"value": value}), 200
            except Exception as e:
                print(f"Exception in get function: {e}")
                return jsonify({"error": f"Unable to fetch the value from node with error: {e}"}), 501
        
        def __set_value(key, value):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
                    if os.name == 'nt':
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # For Windows
                    else:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                    group = struct.pack("4sl", socket.inet_aton(MULTICAST_GROUP), socket.INADDR_ANY)
                    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, group)
                    sock.bind(("", PORT))
                    message = f"AppendSetCommandToLog:{key}:{value}".encode()
                    sock.sendto(message, (MULTICAST_GROUP, PORT))
                    while True:
                        data, _ = sock.recvfrom(1024)
                        message = data.decode()
                        if message.startswith("SetCommand:"):
                            return jsonify({"status": "success"}), 200
            except Exception as e:
                return jsonify({"error": "Unable to set the value"}), 500

        @app.route('/setkey/<key>', methods=['PUT'])
        def setkey(key):
            value = request.json.get("value")
            return __set_value(key, value)

        @app.route('/getkey/<key>', methods=['GET'])
        def getkey(key):
            return __get_value(key)
        
        # @app.route('/show_all', methods = ['GET'])
        # def show_all():
        #     return jsonify({"node_id": node_id, "data": dict(data_store)}), 200
        
        @app.route('/', methods=['GET'])
        def home():
            return f'Welcome to Node {node_id}'
        
        server = make_server('0.0.0.0', port, app)
        server.serve_forever()
    except Exception as e:
        print(f"Error starting Node {node_id} on port {port}: {e}")


        
    


    
    