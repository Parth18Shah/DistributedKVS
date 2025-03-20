from flask import jsonify
import requests
from requests.exceptions import RequestException
import time
from Node.Node import Node_State
from constants import *
from Node.MulticastServer import MulticastServer
from collections import defaultdict
import threading

class NodeManager:
    def __init__(self, request_queue, response_dict, num_nodes=3, num_shards=2):
        self.shards = []
        self.leader_id = []
        self.request_queue = request_queue
        self.response_dict = response_dict
        self.request_id = 0
        for i in range(num_shards):
            self.leader_id.append([-1])
        self.num_shards = num_shards
        for shard_id in range(num_shards):
            nodes = []
            for node_id in range(num_nodes):
                flask_server_port = FLASK_SERVER_PORT + node_id + shard_id * 100
                multicast_port = MULTICAST_PORT + shard_id
                # Creating Multicast Server object which is child of parent class Node
                node = MulticastServer(node_id, flask_server_port, multicast_port, num_nodes, self.leader_id[shard_id])
                nodes.append(node)
            
            # Connecting all nodes to the multicast group
            for i in range(num_nodes):
                nodes[i].join_multicast()

            # Starting the flask servers for each node
            for i in range(num_nodes):
                nodes[i].start_flask_server()
                
            self.shards.append(nodes)

        threading.Thread(target=self.process_request, daemon=True).start()

    def process_request(self):
        while True:
            if not self.request_queue: continue

            request_id,operation, key, value = self.request_queue.popleft()
            if operation == "set":
                response = self.set_values(key, value)
            elif operation == "get":
                response = self.get_value(key)
            elif operation == "delete":
                response = self.delete_value(key)
            print(f"Response for request {request_id}: {response}")
            self.response_dict[request_id] = response

    def add_to_queue(self, operation, key, value):
        self.request_queue.append((self.request_id, operation, key, value))
        self.request_id += 1
        return self.request_id - 1
    
    '''
        Hash function to determine which shard to approach
        (Using FNV Non-Cryptographic Hash Algorithm)
    '''
    def get_shard(self, key):
        hash_value = 2166136261  # Start hash value with FNV offset basis
        fnv_prime = 16777619

        for char in key:
            hash_value ^= ord(char)
            hash_value *= fnv_prime
            hash_value &= 0xFFFFFFFF  # Ensure it remains a 32-bit hash
        return hash_value % self.num_shards

    def get_value(self, key):
        if not key: return {"error": "Please provide a key", "status_code": 400}
        shard_id = self.get_shard(key)
        if self.leader_id[shard_id][0] == -1: return {"error": "Try again later", "status_code": 400}
        for attempt in range(RETRIES_ALLOWED):
            try:
                if self.shards[shard_id][self.leader_id[shard_id][0]].node_state != Node_State(1).name: 
                    time.sleep(5)
                    continue
                leader_port = FLASK_SERVER_PORT + self.leader_id[shard_id][0] + shard_id * 100
                get_response = requests.get(f'http://127.0.0.1:{leader_port}/getkey/{key}')
                if get_response.status_code == 200:
                    get_response = get_response.json()
                    get_response["status_code"] = 200
                    return get_response
            except RequestException as e:
                print(f"Error accessing node {self.leader_id[shard_id][0]}: {e}")
                continue
        return {"error": "Key not found", "status_code": 404}
    
    def set_values(self, key, value):
        if not key: return {"error": "Key not found", "status_code": 400}
        if not value: return {"error": "Value not found", "status_code": 400}
        shard_id = self.get_shard(key)
        if self.leader_id[shard_id][0] == -1: return {"error": "Try again later", "status_code": 400}
        prev_val = None
        get_response = self.get_value(key)
        if get_response["status_code"] == 200:
            prev_val = get_response[key]
        try:
            for attempt in range(RETRIES_ALLOWED):
                if self.shards[shard_id][self.leader_id[shard_id][0]].node_state != Node_State(1).name: 
                    time.sleep(5)
                    continue
                leader_port = FLASK_SERVER_PORT + self.leader_id[shard_id][0]+ shard_id * 100
                set_response = requests.put( f'http://127.0.0.1:{leader_port}/setkey/{key}',json={"value": value})
                if set_response.status_code == 200: return {"status": "success", "message":"Set the value successfully", "status_code": 200}
            if prev_val:
                self.set_values(key, prev_val) 
            return {"error": "Failed to set the value", "status_code": 500}    
        except Exception as e:
            return {"error": "Unable to set the value", "status_code": 500}
    
    def delete_value(self, key):
        if not key: return {"error": "Key not found", "status_code": 400}
        shard_id = self.get_shard(key)
        if self.leader_id[shard_id][0] == -1: return {"error": "Try again later", "status_code": 400}
        prev_val = None
        get_response = self.get_value(key)
        if get_response["status_code"] == 200:
            prev_val = get_response[key]
        else:
            return {"error": "key does not exist in datastore", "status_code": 400}
        try:
            for attempt in range(RETRIES_ALLOWED):
                if self.shards[shard_id][self.leader_id[shard_id][0]].node_state != Node_State(1).name: 
                    time.sleep(5)
                    continue
                leader_port = FLASK_SERVER_PORT + self.leader_id[shard_id][0] + shard_id * 100
                set_response = requests.delete( f'http://127.0.0.1:{leader_port}/deletekey/{key}')
                if set_response.status_code == 200: return {"status": "success", "message":"Deleted the value successfully", "status_code": 200}
            if prev_val:
                self.set_values(key, prev_val) 
            return {"error": "Failed to delete the value", "status_code": 500}       
        except Exception as e:
            return {"error": "Unable to delete the value", "status_code": 500}

    def show_data_from_all_shards(self):
        combined_data = defaultdict(list)
        try:   
            for shard_id, leader_id in enumerate(self.leader_id):
                if leader_id[0] == -1: continue
                leader_port = FLASK_SERVER_PORT + leader_id[0] + shard_id * 100
                show_all_response = requests.get(f'http://127.0.0.1:{leader_port}/show_all')
                if show_all_response.status_code == 200:
                    combined_data.update(show_all_response.json())
                else:
                    combined_data['error'].append(f"Unable to fetch data from Shard {shard_id}")
        except RequestException as e:
            combined_data.append({"error": f"Cannot connect to Shard: {str(e)}"})
        return jsonify({"Combined Data": combined_data}), 200
    
    def stop_nodes(self):        
        print("Stopping all nodes...")
        for groups in self.shards:
            for node in groups:
                node.stop_servers()
        print("All nodes stopped successfully")
        return jsonify({"status": "success"}), 200