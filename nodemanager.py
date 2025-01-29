from Node import Node
from flask import jsonify
import multiprocessing

class NodeManager:
    def __init__(self, num_nodes=3):
        self.nodes = []
        self.queues = []
        for i in range(num_nodes):
            queue = multiprocessing.Queue()
            self.queues.append(queue)
            self.nodes.append(Node(i,queue))
            self.nodes[-1].start()

    def get_value(self, key):
        if not key: return jsonify({"error": "key not found"}), 400
        for queue in self.queues:
            queue.put({"action":"get", "key": key})
            noderesponse = queue.get()
            if noderesponse.get("value") != "Error":
                return jsonify({"value": noderesponse.get("value")}), 200
        return jsonify({"error": "key not found"}), 400
    
    # def get_value(self, key):
    #     if not key: return jsonify({"error": "key not found"}), 400
    #     return self.__get_value(key)
    
    def set_values(self, key, value):
        if not key: return jsonify({"error": "key not found"}), 400
        if not value: return jsonify({"error": "value not found"}), 400
        prev_val = None

        self.queues[0].put({"action":"get", "key": key})
        noderesponse = self.queues[0].get()
        if noderesponse.get("value") != "Error":
            prev_val = noderesponse.get("value")

        for i, queue in enumerate(self.queues):
            queue.put({"action":"set", "key": key, "value": value})
            noderesponse = queue.get()
            if noderesponse.get("status") == "Error":
                if prev_val:
                    for j in range(i):
                        self.queues[j].put({"action":"set", "key": key, "value": prev_val})
                else:
                    # TODO: delete newly added key value pair
                    pass
                return jsonify({"error": "key not found"}), 400
        return jsonify({"status": "success"}), 200

    # def set_values(self, key, value):
    #     if not key: return jsonify({"error": "key not found"}), 400
    #     if not value: return jsonify({"error": "value not found"}), 400
    #     return self.__set_values(key, value)
    
    def show_data_from_all_nodes(self):
        noderesponses = []       
        for queue in self.queues:
            queue.put({"action":"showall"})
            noderesponses.append(queue.get("value"))

        return jsonify({"status": "success", "value":noderesponses}), 200
    
    def stop_nodes(self):
        for queue in self.queues:
            queue.put({"action":"Quit", "key": "", "value": ""})
        
        for node in self.nodes:
            node.join()

        print("All nodes stopped successfully")
        return jsonify({"status": "success"}), 200