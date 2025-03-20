from flask import Flask, request, jsonify
from NodeManager import NodeManager
import atexit
import sys
import queue, time
app = Flask(__name__)

node_manager = None 
request_queue = queue.deque()
responses = {}

def create_server(num_nodes=3, num_shards=2):
    global node_manager
    if node_manager is None:
        node_manager = NodeManager(request_queue, responses, num_nodes, num_shards)
    @app.route('/')
    def home():
        return 'Please use /setkey and /getkey'

    @app.route('/setkey/<key>', methods=['PUT'])
    def setkey(key):
        value = request.json.get("value")
        request_id = node_manager.add_to_queue("set", key, value)
        while request_id not in responses: 
            time.sleep(5) 
        response = responses[request_id]
        status_code = response["status_code"]
        del response["status_code"]
        del responses[request_id]
        return jsonify(response), status_code

    @app.route('/getkey/<key>', methods=['GET'])
    def getkey(key):
        request_id = node_manager.add_to_queue("get", key, None)
        while request_id not in responses: 
            time.sleep(5) 
        response = responses[request_id] 
        status_code = response["status_code"]
        del response["status_code"]
        del responses[request_id]
        return jsonify(response), status_code
    
    @app.route('/deletekey/<key>', methods=['DELETE'])
    def deletekey(key):
        request_id = node_manager.add_to_queue("delete", key, None)
        while request_id not in responses:
            time.sleep(5)
        response = responses[request_id] 
        status_code = response["status_code"]
        del response["status_code"]
        del responses[request_id]
        return jsonify(response), status_code

    @app.route('/show_all', methods = ['GET'])
    def show_all():
        return node_manager.show_data_from_all_shards()

    @app.route('/stop_nodes', methods = ['POST'])
    def stop_nodes():
        return node_manager.stop_nodes()
    
    atexit.register(lambda: node_manager and node_manager.stop_nodes())

    return app

if __name__ == '__main__':
    if len(sys.argv) > 1:
        num_nodes = int(sys.argv[1])
        num_shards = int(sys.argv[2])
    else:
        num_nodes = 3
        num_shards = 2
    app = create_server(num_nodes, num_shards)
    app.run(host='0.0.0.0', port=8000, debug=False)
