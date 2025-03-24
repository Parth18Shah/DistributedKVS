# Distributed Key Value Store

Steps to Setup:

### 1) Creating a virtual environment
``` Bash
python -m venv venv
```

### 2) Installing the necessary packages
``` Bash
pip install -r requirements.txt
```

### 3) Run the Flask server
``` Bash
python server.py
```

### 4) Test using the following commands in the terminal:

- For Adding values to the store
    ``` Bash
    curl -X PUT http://127.0.0.1:8000/setkey/key1 -H "Content-Type: application/json" -d '{"value":"value1"}'

    or 

    curl -X PUT http://127.0.0.1:8000/setkey/key1 -H "Content-Type: application/json" -d "{\"value\":\"value1\"}"
    ```

- For getting a value for a particular key
    ``` Bash
    curl -X GET "http://127.0.0.1:8000/getkey/key1"        
    ```     

- For deleting a value for a particular key
    ``` Bash
    curl -X DELETE "http://127.0.0.1:8000/deletekey/key1"        
    ```    

- To show all the data in all the replicas
    ``` Bash
    curl -X GET "http://127.0.0.1:8000/show_all"        
    ```   

- To test all of them together (using the Test Script we have created)
    - Prerequisite (For RabbitMQ Integration- "queue_br" branch) 
    
        1) Install and Start Docker Engine.

        2) Then, run the following on a terminal
        ``` Bash
        docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4.0-management   
        ``` 

    On a separate terminal: 
    ``` Bash
    python Test.py       
    ``` 

