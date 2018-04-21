from neo4j.v1 import GraphDatabase
from neo4jrestclient import client
def neo4j_test1():
    db = GraphDatabase.driver("bolt://433-18.csse.rose-hulman.edu:7687", auth=("neo4j", "csse433"))
    db.labels.create("Test")
    test = db.nodes.create(name="test")

    db.labels.create("Test2")
    test2 = db.nodes.create(name="test2")

    test.relationships.create("tt", test2)

def neo4j_test2():
    db = GraphDatabase.driver("bolt://433-18.csse.rose-hulman.edu:7687", auth=("neo4j", "csse433"))
    q = 'MATCH (u:Test)-[r:tt]->(m:Tests) WHERE u.name="test" RETURN u, type(r), m'
    results = db.query(q, returns=(client.Node, str, client.Node))
    for r in results:
        print("(%s)-[%s]->(%s)" % (r[0]["name"], r[1], r[2]["name"]))

def neo4j_test3():
    db = GraphDatabase.driver("bolt://433-18.csse.rose-hulman.edu:7687", auth=("neo4j", "csse433"))
    q = 'MATCH (n) DETACH DELETE n'
    db.query(q, returns=(client.Node, str, client.Node))

