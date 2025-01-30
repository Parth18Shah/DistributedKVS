from flask import Flask, request, jsonify
from nodemanager import NodeManager
import os
app = Flask(__name__)

node_manager = NodeManager(num_nodes=3)
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

@app.route('/show_all', methods = ['GET'])
def show_all():
    return node_manager.show_data_from_all_nodes()

@app.route('/stop_nodes', methods = ['POST'])
def stop_nodes():
    return node_manager.stop_nodes()

if __name__ == '__main__':
    print("Server started with pid:- ", os.getpid())
    app.run(debug=True)

