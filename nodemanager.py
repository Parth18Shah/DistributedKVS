import Node from Node

class NodeManager:
    def __init__(self, num_nodes=3):
        self.nodes = []
        for i in range(num_nodes):
            self.nodes[i] = Node(i)

    def __get_value(self, key):
        for node in self.node:
            nodemsg = node.__get_value(key)
            if nodemsg != "Error":
                return jsonify({"value": nodemsg}), 200
        
        return jsonify({"error": "key not found"}), 400
    
    def __set_values(self, key, value):
        prev_val = self.nodes[0].__get_value if self.nodes[0].__get_value!="Error" else None
        for i, node in enumerate(self.nodes):
            nodemsg = node.__set_value(key,value)
            if nodemsg == "Error":
                if prev_val:
                    for j in range(i):
                        self.nodes[j].__set_value(key,prev_val)
                else:
                    # TODO: delete newly added key value pair
                    pass
                return jsonify({"error": "key not found"}), 400
        return jsonify({"status": "success"}), 200