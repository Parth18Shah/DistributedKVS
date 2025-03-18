from flask import jsonify
import requests
from requests.exceptions import RequestException
import time
from Node.Node import Node_State
from constants import *
from Node.MulticastServer import MulticastServer
from collections import defaultdict
class NodeManager:
    def __init__(self, num_nodes=3, num_shards=2):
        self.shards = []
        self.leader_id = []
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

    '''
        Hash function to determine which shard to approach
    '''
    def get_shard(self, key):
        return (ord(key[0].lower()) - 97) % self.num_shards

    def get_value(self, key):
        if not key: return jsonify({"error": "Please provide a key"}), 400
        shard_id = self.get_shard(key)
        if self.leader_id[shard_id][0] == -1: return jsonify({"error": "Try again later"}), 400
        for attempt in range(3):
            try:
                if self.shards[shard_id][self.leader_id[shard_id][0]].node_state != Node_State(1).name: 
                    time.sleep(5)
                    continue
                leader_port = FLASK_SERVER_PORT + self.leader_id[shard_id][0] + shard_id * 100
                get_response = requests.get(f'http://127.0.0.1:{leader_port}/getkey/{key}')
                if get_response.status_code == 200:
                    return get_response.json(), 200
                time.sleep(5)
            except RequestException as e:
                print(f"Error accessing node {self.leader_id[shard_id][0]}: {e}")
                continue
        return jsonify({"error": "Key not found"}), 404
    
    def set_values(self, key, value):
        if not key: return jsonify({"error": "Key not found"}), 400
        if not value: return jsonify({"error": "Value not found"}), 400
        shard_id = self.get_shard(key)
        if self.leader_id[shard_id][0] == -1: return jsonify({"error": "Try again later"}), 400
        prev_val = None
        get_response = self.get_value(key)
        if get_response[1] == 200:
            prev_val = get_response[0].get("value")
        try:
            for attempt in range(3):
                if self.shards[shard_id][self.leader_id[shard_id][0]].node_state != Node_State(1).name: 
                    time.sleep(5)
                    continue
                leader_port = FLASK_SERVER_PORT + self.leader_id[shard_id][0]+ shard_id * 100
                set_response = requests.put( f'http://127.0.0.1:{leader_port}/setkey/{key}',json={"value": value})
                if set_response.status_code == 200: return jsonify({"status": "success", "message":"Set the value successfully"}), 200
            if prev_val:
                self.set_values(key, prev_val) 
            return jsonify({"error": "Failed to set the value"}), 500       
        except Exception as e:
            return jsonify({"error": "Unable to set the value"}), 500
    
    def delete_value(self, key):
        if not key: return jsonify({"error": "Key not found"}), 400
        shard_id = self.get_shard(key)
        if self.leader_id[shard_id][0] == -1: return jsonify({"error": "Try again later"}), 400
        prev_val = None
        get_response = self.get_value(key)
        if get_response[1] == 200:
            prev_val = get_response[0].get("value")
        else:
            return jsonify({"error": "key does not exist in datastore"}), 400
        try:
            for attempt in range(3):
                if self.shards[shard_id][self.leader_id[shard_id][0]].node_state != Node_State(1).name: 
                    time.sleep(5)
                    continue
                leader_port = FLASK_SERVER_PORT + self.leader_id[shard_id][0] + shard_id * 100
                set_response = requests.delete( f'http://127.0.0.1:{leader_port}/deletekey/{key}')
                if set_response.status_code == 200: return jsonify({"status": "success", "message":"Deleted the value successfully"}), 200
            if prev_val:
                self.set_values(key, prev_val) 
            return jsonify({"error": "Failed to delete the value"}), 500       
        except Exception as e:
            return jsonify({"error": "Unable to delete the value"}), 500

    # TODO: Rewrite logic to show all items in datastore
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