from Node import Node
from flask import jsonify
import requests

class NodeManager:
    def __init__(self, num_nodes=3, base_port=8001):
        self.nodes = []
        self.base_port = base_port
        for i in range(num_nodes):
            port = self.base_port + i
            node = Node(i, port)
            self.nodes.append(node)
            node.start()

    def get_value(self, key):
        if not key: return jsonify({"error": "Please provide a key in the request"}), 400
        try:
            for node in self.nodes:
                get_response = requests.get(f'http://127.0.0.1:{node.port}/getkey/{key}')
                if get_response.status_code == 200:
                    return jsonify({"value": get_response.get("value")}), 200
            return jsonify({"error": "Key not found in the data store"}), 400
        except Exception as e:
            return jsonify({"error": "Unable to fetch the value"}), 500
    
    def set_values(self, key, value):
        if not key: return jsonify({"error": "key not found"}), 400
        if not value: return jsonify({"error": "value not found"}), 400
        prev_val = None
        get_response = self.get_value(key)
        if get_response.get("status") == "success":
            prev_val = get_response.get("value")

        try:
            for idx, node in enumerate(self.nodes):
                set_response = requests.put(
                f'http://127.0.0.1:{node.port}/setkey/{key}',
                json={"value": value}
            )
                if set_response.status_code != 200:
                    if prev_val:
                        for j in range(idx):
                            self.nodes[j].set_value(key, prev_val)
                    else:
                        # TODO: delete newly added key value pair
                        pass
                    return jsonify({"error": "Failed to set the value"}), 500        
            return jsonify({"status": "success"}), 200
        except Exception as e:
            return jsonify({"error": "Unable to set the value"}), 500
    
    def show_data_from_all_nodes(self):
        noderesponses = []       
        for idx, node in enumerate(self.nodes):
            show_all_response = requests.get(f'http://127.0.0.1:{node.port}/show_all')
            if show_all_response:
                noderesponses.append(show_all_response.json())
            else:
                noderesponses.append({"error": f"Unable to fetch data from Node {idx}"})
        return jsonify({"status": "success", "value": noderesponses}), 200
    
    def stop_nodes(self):
        print("Stopping all nodes...")
        for node in self.nodes:
            node.stop()

        print("All nodes stopped successfully")
        return jsonify({"status": "success"}), 200