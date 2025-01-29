from flask import Flask, request
from nodemanager import NodeManager
app = Flask(__name__)

node_manager = NodeManager()
@app.route('/')
def home():
    return 'Please use /setkey and /getkey'

@app.route('/setkey/<key>', methods=['PUT'])
def setkey(key):
    print(request.json)
    value = request.json.get('value')
    return node_manager.set_values(key, value)

@app.route('/getkey/<key>', methods=['GET'])
def getkey(key):
    return node_manager.get_value(key)

@app.route('/show_all', methods = ['GET'])
def show_all():
    return node_manager.show_data_from_all_nodes()

if __name__ == '__main__':
    app.run(debug=True)

