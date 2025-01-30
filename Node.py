from flask import Flask, request, jsonify
import threading
import os
import multiprocessing

class Node:
    def __init__(self, node_id, port):
        self.data_store = {}
        self.node_id = node_id
        self.port = port
        # self.app = Flask(__name__)
        # self.__register_routes()
        self.process = multiprocessing.Process(
            target=create_flask_app, 
            args=(self.node_id, self.port, self.data_store)
        )
    
    # def __register_routes(self):
    #     @self.app.route('/setkey/<key>', methods=['PUT'])
    #     def setkey(key):
    #         value = request.json.get("value")
    #         self.__set_value(key, value)
    #         return jsonify({"status": "success"}), 200

    #     @self.app.route('/getkey/<key>', methods=['GET'])
    #     def getkey(key):
    #         return self.__get_value(key)

    #     @self.app.route('/show_all', methods = ['GET'])
    #     def show_all():
    #         return self.__repr__()
    
    # def __get_value(self, key):
    #     if key not in self.data_store:
    #         return jsonify({"error": "key not found"}), 400
    #     return jsonify({"value": self.data_store[key]}), 200
    
    # def __set_value(self, key, value):
    #     self.data_store[key] = value
    
    # def __repr__(self):
    #     return f"Node {self.node_id} Datastore {self.data_store}"

    # def __run_server(self):
    #     self.app.run(host='0.0.0.0', port=self.port)
    #     print(f'Starting node id:- {self.node_id} at port:- {self.port}')

    def start(self):
        print('Starting Node...')
        self.process.start()
        # print(f'Node started with pid:- {os.getpid()} and port :- {self.port}')
    
    def stop(self):
        self.process.terminate() 
        self.process.join()
        return 
    

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
            print("hello to node ", node_id)
            return __get_value(key)
        
        @app.route('/show_all', methods = ['GET'])
        def show_all():
            return jsonify({f"Node {node_id}": data_store}), 200
        
        app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
        print(f'Node {node_id} started with PID: {os.getpid()} and port: {port}')
    except Exception as e:
        print(f"Error starting Node {node_id} on port {port}: {e}")


        
    


    
    