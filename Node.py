class Node:
    def __init__(self, node_id):
        self.data_store = {}
        self.node_id = node_id

    def __get_value(self, key):
        if key not in self.data_store:
            return "Error"
        return self.data_store[key]
    
    def __set_value(self, key, value):
        self.data_store[key] = value

