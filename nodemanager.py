from Node import Node
from flask import jsonify
import requests
from requests.exceptions import RequestException
import time
from Node import Node_State

class NodeManager:
    def __init__(self, num_nodes=3, base_port=8001):
        self.nodes = []
        self.base_port = base_port
        self.leader_id = [-1]
        for i in range(num_nodes):
            port = self.base_port + i
            node = Node(i, port, num_nodes, self.leader_id)
            self.nodes.append(node)

        # Connecting all nodes to the multicast group
        for i in range(num_nodes):
            self.nodes[i].join_multicast()

        # Starting the flask servers for each node
        for i in range(num_nodes):
            self.nodes[i].start()

    def get_value(self, key):
        if not key: return jsonify({"error": "Please provide a key"}), 400
        if self.leader_id[0] == -1: return jsonify({"error": "Try again later"}), 400
        # print(f"current leader is {self.leader_id[0]}")
        for attempt in range(3):
            try:
                if self.nodes[self.leader_id[0]].node_state != Node_State(1).name: 
                    time.sleep(5)
                    continue
                leader_port = self.base_port + self.leader_id[0]
                get_response = requests.get(f'http://127.0.0.1:{leader_port}/getkey/{key}')
                if get_response.status_code == 200:
                    return get_response.json(), 200
                time.sleep(5)
            except RequestException as e:
                print(f"Error accessing node {self.leader_id[0]}: {e}")
                continue
        return jsonify({"error": "Key not found"}), 404
    
    def set_values(self, key, value):
        if not key: return jsonify({"error": "key not found"}), 400
        if not value: return jsonify({"error": "value not found"}), 400
        if self.leader_id[0] == -1: return jsonify({"error": "Try again later"}), 400
        prev_val = None
        get_response = self.get_value(key)
        if get_response[1] == 200:
            prev_val = get_response[0].get("value")
        try:
            for attempt in range(3):
                if self.nodes[self.leader_id[0]].node_state != Node_State(1).name: 
                    time.sleep(5)
                    continue
                leader_port = self.base_port + self.leader_id[0]
                set_response = requests.put( f'http://127.0.0.1:{leader_port}/setkey/{key}',json={"value": value})
                if set_response.status_code == 200: return jsonify({"status": "success"}), 200
            if prev_val:
                self.set_values(key, prev_val) 
            return jsonify({"error": "Failed to set the value"}), 500       
        except Exception as e:
            return jsonify({"error": "Unable to set the value"}), 500
    
    # def show_data_from_all_nodes(self):
    #     noderesponses = []       
    #     for node in self.nodes:
    #         try:
    #             # starttime = time.time()
    #             show_all_response = requests.get(f'http://127.0.0.1:{node.port}/show_all')
    #             # endttime = time.time()
    #             # log_statement = f"\n\nTime taken to fetch data from all nodes is {endttime - starttime}"
    #             # with open("log.txt", "a") as f:
    #             #     f.write(log_statement)
    #             if show_all_response:
    #                 noderesponses.append(show_all_response.json())
    #             else:
    #                 noderesponses.append({"error": f"Unable to fetch data from Node {node.node_id}"})
    #         except RequestException as e:
    #             noderesponses.append({"error": f"Cannot connect to node {node.node_id}: {str(e)}"})
    #     return jsonify({"status": "success", "value": noderesponses}), 200
    
    def stop_nodes(self):        
        print("Stopping all nodes...")
        for node in self.nodes:
            node.stop()

        print("All nodes stopped successfully")
        return jsonify({"status": "success"}), 200