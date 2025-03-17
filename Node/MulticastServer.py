import time
import threading
import socket
import struct
import random
import os
import json
from constants import *
from Node.Node import Node

class MulticastServer(Node):
    def __init__(self, node_id, flask_server_port, multicast_port, node_count, leader_id):
        super().__init__(node_id, flask_server_port, multicast_port, node_count, leader_id)
        self.majority_count = node_count//2 + 1 
        self.votes_received = 0 # Number of votes received for leader election
        self.running = True  # To stop threads gracefully
        self.last_leader_timestamp = time.time()  # Timestamp of the last leader election
        self.election_timeout = random.randint(10,30) # Random election timeout
        self.term = 0 # Current election term
        self.sock = None # Socket for multicast communication
        self.log = [] # List of Log for set commands
        self.ack_rec = 0 # Number of acknowledgements received
    
    def join_multicast(self):
        # Start listening for multicast messages
        threading.Thread(target=self.listen_for_multicast, daemon=True).start()

    def listen_for_multicast(self):
        """
        Listens for incoming multicast messages and responds accordingly.
        """
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        if os.name == 'nt':
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # For Windows
        else:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.sock.bind(("", self.multicast_port))
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
                        if self.votes_received >= self.majority_count:
                            print(f"Node {self.node_id} wins the election and becomes the leader!")
                            self.node_state = Node_State(1).name
                            self.leader_id[0] = self.node_id
                            self.announce_leadership(self.term)
                
                elif message.startswith("AppendSetCommandToLog:"):
                    key, value = message.split(":")[1:]
                    self.log.append((key, value))
                    self.ack_rec = 0
                    message = f"AckSetCommand:{key}:{value}:{self.node_id}".encode()
                    self.sock.sendto(message, (MULTICAST_GROUP, self.multicast_port))
                    
                elif message.startswith("AckSetCommand:"):
                    key, value, msg_from_node_id = message.split(":")[1:]
                    if self.node_id == self.leader_id[0]:
                        self.ack_rec += 1
                        if self.ack_rec >= self.majority_count:
                            self.data_store[key] = value
                            time.sleep(5)
                            if self.log:
                                message = f"SetCommand:{key}:{value}:{self.node_id}".encode()
                                self.sock.sendto(message, (MULTICAST_GROUP, self.multicast_port))
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
                        message = f"GetCommandResponse:{key}:{value}".encode()
                        self.sock.sendto(message, (MULTICAST_GROUP, self.multicast_port))
                
                elif message.startswith("AppendDeleteCommandToLog:"):
                    key = message.split(":")[1]
                    self.log.append((key))
                    self.ack_rec = 0
                    message = f"AckDeleteCommand:{key}:{self.node_id}".encode()
                    self.sock.sendto(message, (MULTICAST_GROUP, self.multicast_port))
                
                elif message.startswith("AckDeleteCommand:"):
                    key, msg_from_node_id = message.split(":")[1:]
                    if self.node_id == self.leader_id[0]:
                        self.ack_rec += 1
                        if self.ack_rec >= self.node_count:
                            if key in self.data_store:
                                del self.data_store[key]
                            time.sleep(5)
                            if self.log:
                                message = f"DeleteCommand:{key}:{self.node_id}".encode()
                                self.sock.sendto(message, (MULTICAST_GROUP, self.multicast_port))
                            self.log = []
                
                elif message.startswith("DeleteCommand:"):
                    key, msg_from_node_id = message.split(":")[1:]
                    print(f"Node {self.node_id} with {self.leader_id} and state {self.node_state} received the command: Delete key {key}")
                    if self.node_id != self.leader_id[0]:
                        if key in self.data_store:
                                del self.data_store[key]
                        self.log = []

                elif message.startswith("RetrieveAllCommand:"):
                    print(f"Node {self.node_id} with {self.leader_id} and state {self.node_state} received the command: get all keys")
                    if self.node_id == self.leader_id[0]:
                        data_store_string = json.dumps(self.data_store)
                        time.sleep(5)
                        message = f"RetrieveAllCommandResponse${data_store_string}".encode()
                        self.sock.sendto(message, (MULTICAST_GROUP, self.multicast_port))

            except socket.timeout:
                if self.node_state == Node_State(1).name:
                    self.node_state = Node_State(3).name
                    self.votes_received = 0

                # No leader detected, check if election is needed
                elif time.time() - self.last_leader_timestamp > self.election_timeout:
                    self.term += 1 
                    self.node_state = Node_State(2).name
                    self.start_election()

    def send_vote(self, candidate_id, addr, current_election_term):
        """
        Sends a unicast vote response to the candidate.
        """
        response = f"VoteGranted:{self.node_id}:{candidate_id}".encode()
        self.sock.sendto(response, (MULTICAST_GROUP, self.multicast_port))
        self.term = current_election_term

    def start_election(self):
        """
        Starts the leader election process.
        """
        self.votes_received = 1  # Vote for itself
        message = f"RequestVote:{self.node_id}:{self.term}".encode()
        self.sock.sendto(message, (MULTICAST_GROUP, self.multicast_port))

    def announce_leadership(self, term):
        """
        Announces that this node has become the leader.
        """
        message = f"LeaderElected:{self.node_id}:{term}".encode()
        self.sock.sendto(message, (MULTICAST_GROUP, self.multicast_port))