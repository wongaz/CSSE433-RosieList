from neo4j_remote import *
import collections
def delete_product(pid):
    product = "p:Product {{ pid:'{0}' }}".format(pid)
    command = "MATCH ({0}) DETACH DELETE p".format(product)
    run_command(command)

def add_product(pid,name,desc,tags,price):
    product = "p:Product {{ pid:'{0}',name:'{1}',desc:'{2}',price:'{3}' }}".format(pid,name,desc,price)
    command = "CREATE ({0}) RETURN p.pid".format(product)
    result = run_command(command)
    for record in result:
        product = record['p.pid']
    if product != pid:
        return 1
    for tag in tags:
        c = "MATCH (a:Product),(b:Tag) WHERE a.pid = '{}' AND b.tgid = '{}' CREATE (a)-[r:Have]->(b) RETURN type(r)".format(product,tag)
        result = run_command(c)
        for record in result:
            if record['type(r)'] != 'Have':
                return 2
    return 0

def update_product_name(pid,name):
    product = "p:Product {{ pid:'{0}' }}".format(pid)
    command = "MATCH ({0}) SET p.name = '{1}' RETURN p.pid".format(product,name)
    run_command(command)

def update_product_desc(pid,desc):
    product = "p:Product {{ pid:'{0}' }}".format(pid)
    command = "MATCH ({0}) SET p.desc = '{1}' RETURN p.pid".format(product,desc)
    run_command(command)

def update_product_price(pid,price):
    product = "p:Product {{ pid:'{0}' }}".format(pid)
    command = "MATCH ({0}) SET p.price = '{1}' RETURN p.pid".format(product,price)
    run_command(command)


def add_tag(tgid,name):
    tag = "t:Tag {{tgid:'{0}',name:'{1}'}}".format(tgid,name)
    command = "CREATE ({0}) RETURN t.tgid".format(tag)
    result = run_command(command)
    for record in result:
        return record['t.tgid']
    return 1

def add_rela(pid,tags):
    for tag in tags.split('|'):
        c = "MATCH (a:Product),(b:Tag) WHERE a.pid = '{0}' AND b.tgid = '{1}' MERGE (a)-[r:Have]->(b) RETURN type(r)".format(pid,tag)
        run_command(c)

def get_recom():
    pid = input("What is the pid you prefer?")
    command = "match (n:Product) -[r:Have]-(t:Tag)-[r2:Have]-(n2:Product) where n.pid='{}' return n2.pid".format(pid)
    result = run_command(command)
    ls = []
    for record in result:
        ls.append(record['n2.pid'])
    print(ls)

def get_recom_sys():
    # can be changed based on popular product (in the future)
    pid= 1
    command = "match (n:Product) -[r:Have]-(t:Tag)-[r2:Have]-(n2:Product) where n.pid='{}' return n2.pid".format(pid)
    result = run_command(command)
    ls = []
    if isinstance(result, collections.Iterable):
        for record in result:
            ls.append(record['n2.pid'])
        if len(ls)>0:
            print("System Recommendations:")
            print(ls)

