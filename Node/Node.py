import multiprocessing
import time
from Node.CreateNodeServer import create_node_server
from constants import *

class Node:
    def __init__(self, node_id, flask_server_port, multicast_port, node_count, leader_id):
        self.data_store = {}
        self.node_id = node_id
        self.flask_server_port = flask_server_port
        self.multicast_port = multicast_port
        self.process = None
        self.node_state = Node_State(3).name
        self.node_count = node_count
        self.leader_id = leader_id
    
    def start_flask_server(self):
        print('Starting Node...')
        self.process = multiprocessing.Process(
            target=create_node_server,
            args=(self.node_id, self.flask_server_port, self.multicast_port)
        )
        self.process.start()
        print(f'Node {self.node_id} started with pid: {self.process.pid} on port: {self.flask_server_port}')
        time.sleep(5)  # Give Flask time to initialize
    
    def stop_servers(self):
        self.running = False
        if self.process and self.process.is_alive():
            self.process.terminate()
            self.process.join()
            print(f'Node {self.node_id} with PID {self.process.pid} stopped.')