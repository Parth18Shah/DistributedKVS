class Node:
    def __init__(self):
        self.data_store = {}

    def __get_value(self, key):
        return self.data_store[key]
    
    def __set_value(self, key, value):
        self.data_store[key] = value

