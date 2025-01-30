url = "http://127.0.0.1:5000/store/"
headers = {
    "Content-Type": "application/json"
}

# Data to send
data = {
    "value": 50
}

# Send the PUT request
try:
    response = requests.put(url+"key1", json=data, headers=headers)
