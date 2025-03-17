from flask import Flask, request, jsonify
from werkzeug.serving import make_server
import socket
import struct
import os
import json
from constants import *

def create_node_server(node_id, flask_server_port, multicast_port):
    
    def __create_socket():
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        if os.name == 'nt':
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # For Windows
        else:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        
        # Join multicast group
        group = struct.pack("4sl", socket.inet_aton(MULTICAST_GROUP), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, group)
        sock.bind(("", multicast_port))
        return sock

    try:
        app = Flask(__name__)

        def __get_value(key):
            try:
                with __create_socket() as sock:
                    message = f"GetCommand:{key}".encode()
                    sock.sendto(message, (MULTICAST_GROUP, multicast_port))
                    while True:
                        data, _ = sock.recvfrom(1024)
                        message = data.decode()
                        if message.startswith("GetCommandResponse:"):
                            retrieved_key, value = message.split(":")[1:]
                            if key == retrieved_key:
                                if value == "Key not found": 
                                    return jsonify({"error": "key not found"}), 400
                                return jsonify({"value": value}), 200
            except Exception as e:
                print(f"Exception in get function: {e}")
                return jsonify({"error": f"Unable to fetch the value from node with error: {e}"}), 501
        
        def __set_value(key, value):
            try:
                with __create_socket() as sock:
                    message = f"AppendSetCommandToLog:{key}:{value}".encode()
                    sock.sendto(message, (MULTICAST_GROUP, multicast_port))
                    while True:
                        data, _ = sock.recvfrom(1024)
                        message = data.decode()
                        if message.startswith("SetCommand:"):
                            return jsonify({"status": "success"}), 200
            except Exception as e:
                return jsonify({"error": "Unable to set the value"}), 500

        def __delete_value(key):
            try:
                with __create_socket() as sock:
                    message = f"AppendDeleteCommandToLog:{key}".encode()
                    sock.sendto(message, (MULTICAST_GROUP, multicast_port))
                    while True:
                        data, _ = sock.recvfrom(1024)
                        message = data.decode()
                        if message.startswith("DeleteCommand:"):
                            return jsonify({"status": "success"}), 200
            except Exception as e:
                return jsonify({"error": "Unable to delete the value"}), 500

        @app.route('/setkey/<key>', methods=['PUT'])
        def setkey(key):
            value = request.json.get("value")
            return __set_value(key, value)

        @app.route('/getkey/<key>', methods=['GET'])
        def getkey(key):
            return __get_value(key)
        
        @app.route('/deletekey/<key>', methods=['DELETE'])
        def deletekey(key):
            return __delete_value(key)
        
        @app.route('/show_all', methods = ['GET'])
        def show_all():
            try:
                with __create_socket() as sock:
                    message = f"RetrieveAllCommand:".encode()
                    sock.sendto(message, (MULTICAST_GROUP, multicast_port))
                    while True:
                        data, _ = sock.recvfrom(10000)
                        message = data.decode()
                        if message.startswith("RetrieveAllCommandResponse"):
                            string_data = message.split("$")[1]
                            retrieved_values = json.loads(string_data)
                            return jsonify(retrieved_values), 200
            except Exception as e:
                print(f"Exception in get function: {e}")
                return jsonify({"error": f"Unable to fetch the value from node with error: {e}"}), 501
        
        @app.route('/', methods=['GET'])
        def home():
            return f'Welcome to Node {node_id}'
        
        server = make_server('0.0.0.0', flask_server_port, app)
        server.serve_forever()
    except Exception as e:
        print(f"Error starting Node {node_id} on port {flask_server_port}: {e}")