# CSSE433-RosieList

## How to test connection:
Clone the project and run `pip install -r requirements.txt` for node 18 and 19;
run `pip3 install -r requirements.txt` for node 17

start redis on node 17 and 19
start neo4j on node 18 

run `python Queue_server.py` on node 19

run `python3 FE_server` on node 17 and type 'test'

If you see three tests with correct results, the connections are solid

