from neo4j_remote import *



def add_product(pid,name,desc,tags,price):
    product = "p:Product {{ pid:'{0}',name:'{1}',desc:'{2}',price:'{3}' }}".format(pid,name,desc,price)
    command = "CREATE ({0}) RETURN p.pid".format(product)
    result = run_command(command)
    for record  in result:
        product =  record['p.pid']
    if product != pid:
        return 1
    for tag in tags:
        c = "MATCH (a:Product),(b:Tag) WHERE a.pid = '{}' AND b.tgid = '{}' CREATE (a)-[r:Have]-(b) RETURN type(r)".format(product,tag)
        result = run_command(c)
        for record in result:
            if record['type(r)'] != 'Have':
                return 2

def add_tag(tgid,name):
    tag = "t:Tag {{tgid:'{0}',name:'{1}'}}".format(tgid,name)
    command = "CREATE ({0}) RETURN t.tgid".format(tag)
    result = run_command(command)
    for record  in result:
        return record['t.tgid']
    return 1

