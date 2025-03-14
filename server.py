from flask import Flask, request
from nodemanager import NodeManager
import atexit
import sys

app = Flask(__name__)

node_manager = None 

def create_server(num_nodes=3, num_groups=2):
    global node_manager
    if node_manager is None:
        node_manager = NodeManager(num_nodes, num_groups)
    @app.route('/')
    def home():
        return 'Please use /setkey and /getkey'

    @app.route('/setkey/<key>', methods=['PUT'])
    def setkey(key):
        value = request.json.get("value")
        return node_manager.set_values(key, value)

    @app.route('/getkey/<key>', methods=['GET'])
    def getkey(key):
        return node_manager.get_value(key)
    
    @app.route('/deletekey/<key>', methods=['DELETE'])
    def deletekey(key):
        return node_manager.delete_value(key)

    @app.route('/show_all', methods = ['GET'])
    def show_all():
        return node_manager.show_data_from_all_nodes()

    @app.route('/stop_nodes', methods = ['POST'])
    def stop_nodes():
        return node_manager.stop_nodes()
    
    atexit.register(lambda: node_manager and node_manager.stop_nodes())

    return app

if __name__ == '__main__':
    # multiprocessing.freeze_support()  # Required for Windows
    if len(sys.argv) > 1:
        num_nodes = int(sys.argv[1])
        num_groups = int(sys.argv[2])
    else:
        num_nodes = 3
        num_groups = 2
    app = create_server(num_nodes, num_groups)
    app.run(host='0.0.0.0', port=8000, debug=False)
