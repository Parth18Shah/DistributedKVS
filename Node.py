import multiprocessing

class Node(multiprocessing.Process):
    def __init__(self, node_id, queue):
        super().__init__()
        self.data_store = {}
        self.node_id = node_id
        self.queue = queue
    
    def run(self):
        print(f'Starting node id:- {self.node_id}')
        while True:
            task = self.queue.get()
            action = task.get("action")
            key = task.get("key")
            value = task.get("value")
            try:
                if action == "set":
                    self.__set_value(key,value)
                    self.queue.put({"status": "success"})
                elif action == "get":
                    response = self.__get_value(key)
                    self.queue.put({"status": "success", "value": response})
                elif action == "showall":
                    response = self.__repr__()
                    self.queue.put({"status": "success", "value": response})
                else:
                    print(f'Terminating node id:- {self.node_id}')
                    break
            except Exception as e:
                self.queue.put({"status": "Error", "value": str(e)})
                break
            

    def __get_value(self, key):
        if key not in self.data_store:
            return "Error"
        return self.data_store[key]
    
    # def get_value(self, key):
    #     return self.__get_value(key)
    
    def __set_value(self, key, value):
        self.data_store[key] = value
    
    # def set_value(self, key, value):
    #     return self.__set_value(key, value)
    
    def __repr__(self):
        return f"Node {self.node_id} Datastore {self.data_store}"
