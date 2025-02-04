Distributed Key Value Store

Steps to Setup

## 1) Creating a virtual environment
``` Bash
python -m venv venv
```

## 2) Installing the necessary packages and creating protocol buffer files for Python
``` Bash
pip install -r requirements.txt

python -m pip install grpcio grpcio-tools

python -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. ./service.proto
```

## 3) Run the Flask server
``` Bash
python server.py
```

## 4) Test using the following commands in the terminal:

- For Adding values to the store
``` Bash
curl -X PUT http://127.0.0.1:5000/setkey/key1 -H "Content-Type: application/json" -d '{"value":"value1"}'

or 

curl -X PUT http://127.0.0.1:5000/setkey/key1 -H "Content-Type: application/json" -d "{\"value\":\"value1\"}"
```

- For getting a value for a particular key
``` Bash
curl -X GET "http://127.0.0.1:5000/getkey/key1"        
```     

- To show all the data in all the replicas
``` Bash
curl -X GET "http://127.0.0.1:5000/show_all"        
```  

- To stop nodes gracefully
``` Bash
curl -X GET "http://127.0.0.1:5000/stop_nodes"        
```  