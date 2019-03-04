# CSSE433-RosieList

## How to test connection:
Clone the project and run `pip install -r requirements.txt` for node 17, 18 and 19;

start redis on node 17 and 19

start neo4j on node 18 

run `python Queue_server.py` on node 19

run `python frontend` on any machine to start the interface

## Project Description
Our take on sharepoint. This syst4em features 3 No-SQL databases with a simple pub sub system for persistence. 

## Architecture
3 No-SQL Databases
- Neo4J
- Redis 
- Google BigTable 

Each of the databases had there own inherent benefits to the system. Redis served as a cache layer for users recently viewed items and as the pub sub system. Neo4J was used for doing n-degrees of separations recommendation engine. Google BigTables acted as the primary storage for our system. Althought BigTable was used as the primary storage, the system will function if any of the components goes down. 

Between BigTable and Neo4J all reads can be performed by either system for simplicity but if a write needs to occur we have RQ in place for a message queing bus and persistence. 
