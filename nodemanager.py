from Node import Node
from flask import jsonify

class NodeManager:
    def __init__(self, num_nodes=3):
        self.nodes = []
        for i in range(num_nodes):
            self.nodes.append(Node(i))

    def __get_value(self, key):
        for node in self.nodes:
            nodemsg = node.get_value(key)
            if nodemsg != "Error":
                return jsonify({"value": nodemsg}), 200
        return jsonify({"error": "key not found"}), 400
    
    def get_value(self, key):
        if not key: return jsonify({"error": "key not found"}), 400
        return self.__get_value(key)
    
    def __set_values(self, key, value):
        prev_val = self.nodes[0].get_value if self.nodes[0].get_value!="Error" else None
        for i, node in enumerate(self.nodes):
            nodemsg = node.set_value(key,value)
            if nodemsg == "Error":
                if prev_val:
                    for j in range(i):
                        self.nodes[j].set_value(key,prev_val)
                else:
                    # TODO: delete newly added key value pair
                    pass
                return jsonify({"error": "key not found"}), 400
        return jsonify({"status": "success"}), 200

    def set_values(self, key, value):
        if not key: return jsonify({"error": "key not found"}), 400
        if not value: return jsonify({"error": "value not found"}), 400
        return self.__set_values(key, value)
    
    def show_data_from_all_nodes(self):
        for node in self.nodes:
            print(f' Node {node.node_id}: {node.data_store}')
        return jsonify({"status": "success"}), 200