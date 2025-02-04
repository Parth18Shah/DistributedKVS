from Node import Node
from flask import jsonify
from requests.exceptions import RequestException
import grpc
import service_pb2
import service_pb2_grpc


class NodeManager:
    def __init__(self, num_nodes=3, base_port=8001):
        self.nodes = []
        self.base_port = base_port
        for i in range(num_nodes):
            port = self.base_port + i
            node = Node(i, port)
            self.nodes.append(node)
            node.start()
    
    # TODO: To be used in future as Controller
    # def wait_for_nodes(self, timeout=10):
    #     start_time = time.time()
    #     while time.time() - start_time < timeout:
    #         all_nodes_started = True
    #         for node in self.nodes:
    #             try:
    #                 response = requests.get(f'http://127.0.0.1:{node.port}/health')
    #                 if response.status_code != 200:
    #                     all_nodes_started = False
    #                     break
    #             except RequestException:
    #                 all_nodes_started = False
    #                 break
    #         if all_nodes_started:
    #             print("All nodes are ready!")
    #             return True
    #         time.sleep(0.5)
    #     raise Exception("Timeout waiting for nodes to start")

    def get_value(self, key):
        if not key: return jsonify({"error": "Please provide a key"}), 400
        for node in self.nodes:
            try:
                with grpc.insecure_channel("localhost:"+str(node.port)) as channel:
                    stub = service_pb2_grpc.KeyValueStoreStub(channel)
                    get_response = stub.RpcGetValue(service_pb2.RpcGetRequest(key=key))
                    if get_response.value != "Error":
                        return {"value":get_response.value}, 200
            except RequestException as e:
                print(f"Error accessing node {node.node_id}: {e}")
                continue
        return jsonify({"error": "Key not found"}), 404
    
    def set_values(self, key, value):
        if not key: return jsonify({"error": "key not found"}), 400
        if not value: return jsonify({"error": "value not found"}), 400
        prev_val = None
        get_response = self.get_value(key)
        print(get_response)
        if value in get_response:
            prev_val = get_response.get("value")
        try:
            for idx, node in enumerate(self.nodes):
                with grpc.insecure_channel("localhost:"+str(node.port)) as channel:
                    stub = service_pb2_grpc.KeyValueStoreStub(channel)
                    set_response = stub.RpcSetValue(service_pb2.RpcSetRequest(key=key, value=value))
                    if not set_response.output:
                        if prev_val:
                            for j in range(idx):
                                with grpc.insecure_channel("localhost:"+str(self.nodes[j].port)) as channel:
                                    stub = service_pb2_grpc.KeyValueStoreStub(channel)
                                    set_response = stub.RpcSetValue(service_pb2.RpcSetRequest(key=key, value=prev_val))
                        else:
                            # TODO: delete newly added key value pair
                            pass
                        return jsonify({"error": "Failed to set the value"}), 500        
            return jsonify({"status": "success"}), 200
        except Exception as e:
            return jsonify({"error": "Unable to set the value"}), 500
    
    def show_data_from_all_nodes(self):
        noderesponses = {}       
        for node in self.nodes:
            try:
                with grpc.insecure_channel("localhost:"+str(node.port)) as channel:
                    stub = service_pb2_grpc.KeyValueStoreStub(channel)
                    showall_response = stub.RpcShowAll(service_pb2.RpcShowAllRequest())
                    node_id_str = f"node {showall_response.node_id}"
                    if "Error" not in showall_response.data:
                        noderesponses[node_id_str] = dict(showall_response.data)
                    else:
                        noderesponses[node_id_str] = "Unable to fetch data from Node"
            except RequestException as e:
                noderesponses[node_id_str] = f"Cannot connect to node: {str(e)}"             
        return jsonify(noderesponses), 200
    
    def stop_nodes(self):        
        print("Stopping all nodes...")
        for node in self.nodes:
            node.stop()

        print("All nodes stopped successfully")
        return jsonify({"status": "success"}), 200