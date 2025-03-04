from flask import Flask, request, jsonify
import multiprocessing
import time
from werkzeug.serving import make_server
from enum import Enum
import threading
import socket
import struct
import random

class Node_State(Enum):
    Leader= 1
    Candidate = 2
    Follower = 3

MULTICAST_GROUP = "224.1.1.1"
PORT = 5007

class Node:
    def __init__(self, node_id, port, node_count):
        self.data_store = {}
        self.node_id = node_id
        self.port = port
        self.process = None
        self.node_state = Node_State(3).name
        self.node_count = node_count
        self.majority_count = node_count//2 + 1
        self.votes_received = 0
        self.running = True  # To stop threads gracefully
        self.last_leader_timestamp = time.time()
        self.election_timeout = random.randint(10,30)
        self.term = 0
        self.sock = None
    
    def start(self):
        print('Starting Node...')
        self.process = multiprocessing.Process(
            target=create_flask_app,
            args=(self.node_id, self.port, self.data_store)
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
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("", PORT))
        print(f"Node {self.node_id} Connected to Multicast socket")
        # Join multicast group
        mreq = struct.pack("4sl", socket.inet_aton(MULTICAST_GROUP), socket.INADDR_ANY)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        while self.running:
            print(f"Node ID {self.node_id} and node state {self.node_state}")
            if self.term == 0:
                self.sock.settimeout(self.election_timeout)
            else:
                self.sock.settimeout(600+random.randint(10,30))
            try:
                data, addr = self.sock.recvfrom(1024)
                message = data.decode()
                print(f"Node {self.node_id} received: {message}")

                if message.startswith("RequestVote:"):
                    msg = message.split(":")
                    candidate_id = int(msg[1])
                    current_election_term = int(msg[2])
                    if self.node_state == Node_State(3).name and current_election_term>self.term:
                        self.send_vote(candidate_id, addr, current_election_term)

                elif message.startswith("LeaderElected:"):
                    leader_id = int(message.split(":")[1])
                    print(f"Node {self.node_id} recognizes Leader {leader_id} for the term {self.term}")
                    if leader_id != self.node_id:
                        print(f"Node {self.node_id} recognizes Leader {leader_id}")
                        self.node_state = Node_State(3).name
                        print(f"Node:- {self.node_id} state set to {self.node_state}")
                        self.last_leader_timestamp = time.time()
                
                elif message.startswith("VoteGranted:"):
                    vote_by_node_id, vote_for_node_id = map(int, message.split(":")[1:])
                    print(f"Node {self.node_id}: vote by :- {vote_by_node_id} and vote for:- {vote_for_node_id}")
                    if self.node_id == vote_for_node_id:
                        self.votes_received += 1
                        print(f"Node {self.node_id} received a vote! Total: {self.votes_received}")

                        if self.votes_received >= self.majority_count:
                            print(f"Node {self.node_id} wins the election and becomes the leader!")
                            self.node_state = "leader"
                            self.announce_leadership()

            except socket.timeout:
                if self.node_state == Node_State(1).name:
                    self.node_state = Node_State(3).name
                    print(f"Node:- {self.node_id} state set to {self.node_state}")
                # No leader message received, check if election is needed
                elif time.time() - self.last_leader_timestamp > self.election_timeout:
                    print(f"Node {self.node_id} detected no leader. Starting election...")
                    self.term += 1 
                    self.node_state = Node_State(2).name
                    print(f"Node:- {self.node_id} state set to {self.node_state}")
                    self.start_election()
    
    def send_vote(self, candidate_id, addr, current_election_term):
        """
        Sends a unicast vote response to the candidate.
        """
        print(f"Node {self.node_id} voting for Candidate {candidate_id}")
        response = f"VoteGranted:{self.node_id}:{candidate_id}".encode()
        print(response)
        self.sock.sendto(response, addr)
        self.term = current_election_term
        print("The message vote granted is sent")

    def start_election(self):
        """
        Starts the leader election process.
        """
        self.votes_received = 1  # Vote for itself
        print(f"Node {self.node_id} is running for election.")
        message = f"RequestVote:{self.node_id}:{self.term}".encode()
        print(message)
        self.sock.sendto(message, (MULTICAST_GROUP, PORT))

        # #Wait for votes
        # start_time = time.time()
        # while time.time() - start_time < self.election_timeout:
        #     data, _ = self.sock.recvfrom(1024)
        #     response = data.decode()
        #     if response.startswith("VoteGranted:"):
        #         self.votes_received += 1
        #         print(f"Node {self.node_id} received a vote! Total: {self.votes_received}")

        #         if self.votes_received >= self.majority_count:
        #             print(f"Node {self.node_id} wins the election and becomes the leader!")
        #             self.node_state = "leader"
        #             self.announce_leadership()
        #             return

        # print(f"Node {self.node_id} failed to get majority votes, retrying election...")
        # if self.node_state==Node_State(2).name:
        #     self.start_election()
    
    def announce_leadership(self):
        """
        Announces that this node has become the leader.
        """
        message = f"LeaderElected:{self.node_id}".encode()
        print(message)
        self.sock.sendto(message, (MULTICAST_GROUP, PORT))


def create_flask_app(node_id, port, data_store):
    try:
        app = Flask(__name__)

        def __get_value(key):
            if key not in data_store:
                return jsonify({"error": "key not found"}), 400
            return jsonify({"value": data_store[key]}), 200
        
        def __set_value(key, value):
            data_store[key] = value

        @app.route('/setkey/<key>', methods=['PUT'])
        def setkey(key):
            value = request.json.get("value")
            __set_value(key, value)
            return jsonify({"status": "success"}), 200

        @app.route('/getkey/<key>', methods=['GET'])
        def getkey(key):
            return __get_value(key)
        
        @app.route('/show_all', methods = ['GET'])
        def show_all():
            return jsonify({"node_id": node_id, "data": dict(data_store)}), 200

        # @app.route('/health', methods=['GET'])
        # def health_check(): 
        #     return jsonify({"status": "healthy", "node_id": node_id}), 200
        
        @app.route('/', methods=['GET'])
        def home():
            return f'Welcome to Node {node_id}'
        
        server = make_server('0.0.0.0', port, app)
        server.serve_forever()
    except Exception as e:
        print(f"Error starting Node {node_id} on port {port}: {e}")


        
    


    
    