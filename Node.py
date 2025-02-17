import grpc
import service_pb2
import service_pb2_grpc
from concurrent import futures

class KeyValueStoreServicer(service_pb2_grpc.KeyValueStoreServicer):

    def __init__(self, data_store, node_id):
        self.data_store = data_store 
        self.node_id = node_id

    def RpcGetValue(self, request, context):
        return service_pb2.RpcGetResponse(value=self.data_store.get(request.key,"Error"))

    def RpcSetValue(self, request, context):
        try:
            value = request.value
            key = request.key
            self.data_store[key] = value
            return service_pb2.RpcSetResponse(output=True)
        except:
            return service_pb2.RpcSetResponse(output=False)

    def RpcShowAll(self, request, context):
        return service_pb2.RpcShowAllResponse(node_id=self.node_id, data=dict(self.data_store))
    

class Node:
    def __init__(self, node_id, port):
        self.data_store = {}
        self.node_id = node_id
        self.port = port
        self.server = None
    
    def start(self):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        service_pb2_grpc.add_KeyValueStoreServicer_to_server(KeyValueStoreServicer(self.data_store, self.node_id), self.server)
        self.server.add_insecure_port(f"[::]:{self.port}")
        self.server.start()
        print(f'Node {self.node_id} started on port:{self.port}')


    def stop(self):
        if self.server:
            print(f'Node {self.node_id} stopped.')
            self.server.stop(0)


        
    


    
    