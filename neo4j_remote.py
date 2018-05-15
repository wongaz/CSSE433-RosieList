import neo4j
from neo4j.v1 import GraphDatabase, basic_auth
from functools import wraps


def neo4j_connect(func):
    @wraps(func)
    def function_wrapper(*args, **kwargs):
        driver = GraphDatabase.driver("bolt://433-18.csse.rose-hulman.edu", auth=basic_auth("neo4j", "csse433"))
        session = driver.session()
        r = func(session, *args, **kwargs)
        session.close()
        return r
    return function_wrapper


@neo4j_connect
def run_command(session, command):
    try:
        return session.run(command)
    except:
        print("Neo4J is not available, but all the changes will be made once its up")
        return 1


