from enum import Enum

# Enum for node states
class Node_State(Enum):
    Leader = 1
    Candidate = 2
    Follower = 3

# Constants for multicast communication
MULTICAST_GROUP = "224.1.1.1"

# Constant Port Values
FLASK_SERVER_PORT = 8001
MULTICAST_PORT = 5007

RETRIES_ALLOWED = 3 # Number of times to retry a request