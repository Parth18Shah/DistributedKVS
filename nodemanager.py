from Node import Node
from flask import jsonify
import requests
from requests.exceptions import RequestException

class NodeManager:
    def __init__(self, num_nodes=3, base_port=8001):
        self.nodes = []
        self.base_port = base_port
        for i in range(num_nodes):
            port = self.base_port + i
            node = Node(i, port)
            self.nodes.append(node)
            node.start()
        # self.wait_for_nodes()
    
    # TODO: To be used in future as Controller
    # def wait_for_nodes(self, timeout=10):
    #     start_time = time.time()
    #     while time.time() - start_time < timeout:
    #         all_nodes_started = True
    #         for node in self.nodes:
    #             try:
    #                 response = requests.get(f'http://127.0.0.1:{node.port}/health')
    #                 if response.status_code != 200:
    #                     all_nodes_started = False
    #                     break
    #             except RequestException:
    #                 all_nodes_started = False
    #                 break
    #         if all_nodes_started:
    #             print("All nodes are ready!")
    #             return True
    #         time.sleep(0.5)
    #     raise Exception("Timeout waiting for nodes to start")

    def get_value(self, key):
        if not key: return jsonify({"error": "Please provide a key"}), 400
        for node in self.nodes:
            try:
                get_response = requests.get(f'http://127.0.0.1:{node.port}/getkey/{key}')
                if get_response.status_code == 200:
                    return get_response.json(), 200
            except RequestException as e:
                print(f"Error accessing node {node.node_id}: {e}")
                continue
        return jsonify({"error": "Key not found"}), 404
    
    def set_values(self, key, value):
        if not key: return jsonify({"error": "key not found"}), 400
        if not value: return jsonify({"error": "value not found"}), 400
        prev_val = None
        get_response = self.get_value(key)
        if get_response[1] == 200:
            prev_val = get_response[0].get("value")

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
        for node in self.nodes:
            try:
                show_all_response = requests.get(f'http://127.0.0.1:{node.port}/show_all')
                if show_all_response:
                    noderesponses.append(show_all_response.json())
                else:
                    noderesponses.append({"error": f"Unable to fetch data from Node {node.node_id}"})
            except RequestException as e:
                noderesponses.append({"error": f"Cannot connect to node {node.node_id}: {str(e)}"})
        return jsonify({"status": "success", "value": noderesponses}), 200
    
    def stop_nodes(self):        
        print("Stopping all nodes...")
        for node in self.nodes:
            node.stop()

        print("All nodes stopped successfully")
        return jsonify({"status": "success"}), 200