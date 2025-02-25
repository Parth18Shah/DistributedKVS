import random
import time
from faker import Faker
import os
import subprocess
import atexit
import pandas as pd
import sys

fake = Faker()
data = {}
NUMBER_OF_TESTCASES = 1000
NUMBER_OF_NODES = 3
processes = []

def generate_data():
    '''
        Description: Generates mock data for testing
    '''
    for idx in range(NUMBER_OF_TESTCASES):
        data["country"+str(idx)] = fake.country()
    return data

def get_input():
    '''
        Description: Gets the input from the user and sets the global variables
    '''
    global NUMBER_OF_TESTCASES
    global NUMBER_OF_NODES
    testcase_count = input("Enter the number of test cases:")
    nodes_count = input("Enter the number of nodes:")

    if testcase_count and testcase_count.isdigit():
        NUMBER_OF_TESTCASES = int(testcase_count)
    if nodes_count and nodes_count.isdigit():
        NUMBER_OF_NODES = int(nodes_count)

def calc_aggregate_time():
    '''
        Description: Calculates the aggregate time for each operation
    '''
    total_set_time = 0
    set_operation_calls = 0
    total_get_time = 0
    get_operation_calls = 0
    total_show_all_time = 0
    show_all_operation_calls = 0

    with open("log.txt", "r") as f:
        for line in f:
            if "set" in line:
                total_set_time += float(line.split('is')[-1].strip())
                set_operation_calls += 1
            elif "get" in line:
                total_get_time += float(line.split('is')[-1].strip())
                get_operation_calls += 1
            elif "all" in line:
                total_show_all_time += float(line.split('is')[-1].strip())
                show_all_operation_calls += 1

    df = pd.DataFrame({
        "set_time": [total_set_time],
        "set_calls_count": [set_operation_calls],
        "aggregate_set_time": [total_set_time/set_operation_calls],
        "get_time": [total_get_time],
        "get_calls_count": [get_operation_calls],
        "aggregate_get_time": [total_get_time/get_operation_calls],
        "show_all_time": [total_show_all_time],
        "show_all_calls_count": [show_all_operation_calls],
        "aggregate_show_all_time": [total_show_all_time/show_all_operation_calls]
    })

    df.to_csv('aggregate_time.csv', index=False)


def test_server():
    '''
        Description: Simulates the read and write operations on the data store
    '''
    try:
        data = generate_data()
        # Starting the server
        # For Mac
        # server_process = subprocess.Popen(["python", "server.py", str(NUMBER_OF_NODES)])
        # For Windows
        python_executable = sys.executable 
        server_process = subprocess.Popen([python_executable, "server.py", str(NUMBER_OF_NODES)])
        processes.append(server_process)
        time.sleep(5)

        # Setting the values
        for key, value in data.items():
            # For Mac
            #set_command = f"curl -X PUT http://127.0.0.1:8000/setkey/{key} -H \"Content-Type:application/json\" -d '{{\"value\": \"{value}\"}}'"
            # For Windows
            set_command = f'curl -X PUT http://127.0.0.1:8000/setkey/{key} -H "Content-Type:application/json" -d "{{\\"value\\": \\"{value}\\"}}"'
            os.system(set_command)
        time.sleep(5)

        print('\n Done with setting values')
        print('\nChecking the get and show all commands')

        # Getting the values
        for _ in range(NUMBER_OF_TESTCASES * 2):
            random_key = random.choice(list(data.keys()))
            get_command =  f"curl -X GET http://127.0.0.1:8000/getkey/{random_key}"
            os.system(get_command)

        time.sleep(5)
        # Checking the show all command
        show_all_command =  f"curl -X GET http://127.0.0.1:8000/show_all"
        os.system(show_all_command)
        
    except:
        with open("log.txt", "a") as f:
            f.write("\nTest failed")
        print("Test failed")
        cleanup()


def cleanup():
    '''
        Description: Prevents the server from running in the background after this script is terminated 
    '''
    print("Test completed")
    for process in processes:
        process.terminate()

# Registering the cleanup function
atexit.register(cleanup)

if __name__ == "__main__":
    get_input()
    test_server()
    calc_aggregate_time()