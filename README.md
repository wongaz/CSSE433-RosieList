# CSSE433-RosieList

## How to test connection:
Clone the project and run `pip install -r requirements.txt` for node 17, 18 and 19;

start redis on node 17 and 19

start neo4j on node 18 

run `python Queue_server.py` on node 19

run `python frontend` on any machine to start the interface
(This can show connection to bigtable is good)

run 'python FE_server' in `retired` folder and type `test` to show neo4j and redis connections are good
(This is just a POC, neo4j and redis features are not implemented yet )


