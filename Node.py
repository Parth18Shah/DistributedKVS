from flask import Flask, request, jsonify
import multiprocessing
import time
from werkzeug.serving import make_server

class Node:
    def __init__(self, node_id, port):
        self.data_store = {}
        self.node_id = node_id
        self.port = port
        self.process = None
    
    def start(self):
        print('Starting Node...')
        self.process = multiprocessing.Process(
            target=create_flask_app,
            args=(self.node_id, self.port, self.data_store)
        )
        self.process.start()
        time.sleep(1)  # Give Flask time to initialize
        print(f'Node {self.node_id} started with pid: {self.process.pid} on port: {self.port}')
    
    def stop(self):
        if self.process and self.process.is_alive():
            self.process.terminate()
            self.process.join()
            print(f'Node {self.node_id} with PID {self.process.pid} stopped.')

def create_flask_app(node_id, port, data_store):
    try:
        app = Flask(__name__)

        def __get_value(key):
            if key not in data_store:
                return jsonify({"error": "key not found"}), 400
            return jsonify({"value": data_store[key]}), 200
        
        def __set_value(key, value):
            data_store[key] = value

        @app.route('/setkey/<key>', methods=['PUT'])
        def setkey(key):
            value = request.json.get("value")
            __set_value(key, value)
            return jsonify({"status": "success"}), 200

        @app.route('/getkey/<key>', methods=['GET'])
        def getkey(key):
            return __get_value(key)
        
        @app.route('/show_all', methods = ['GET'])
        def show_all():
            return jsonify({"node_id": node_id, "data": dict(data_store)}), 200

        @app.route('/health', methods=['GET'])
        def health_check(): 
            return jsonify({"status": "healthy", "node_id": node_id}), 200
        
        @app.route('/', methods=['GET'])
        def home():
            return f'Welcome to Node {node_id}'
        
        server = make_server('0.0.0.0', port, app)
        server.serve_forever()
    except Exception as e:
        print(f"Error starting Node {node_id} on port {port}: {e}")


        
    


    
    