import redis
import os
from google.cloud import bigtable
from google.cloud import happybase
from csv import reader
from rq import Queue
from time import sleep
##############connection setup####################################

cache_conn = redis.StrictRedis(host='433-17.csse.rose-hulman.edu', port=6379, db=0)
queue_conn = redis.StrictRedis(host='433-18.csse.rose-hulman.edu', port=6379, db=0)
q = Queue(connection=queue_conn)



#########interface functionality#########################

def dataflow_test(_):
    cache_conn.set('test', 'cache_write')
    print('write to cache server...')
    result = cache_conn.get('test')
    print('retrive value from cache server...')
    print('get result: ' + result.decode("utf-8"))
    print('----------------------------------------')
    q.enqueue(bigtable_test1)
    sleep(5)
    bigtable_test2()
    q.enqueue(bigtable_test3)


def cache_flushall(_):
    return cache_conn.flushall()


############bigtable function##########################

def bigtable_test1():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "key.json"
    project_id= "csse433-adb-201117"
    instance_id ="project433"
    table_name = 'test'

    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    connection = happybase.Connection(instance=instance)

    print('Creating the {} table.'.format(table_name))
    column_family_name = 'test_cf'
    connection.create_table(
        table_name,
        {
            column_family_name: dict()  # Use default options.
        })

    print('Writing some greetings to the table.')
    table = connection.table(table_name)
    column_name = '{fam}:test_c'.format(fam=column_family_name)
    row = [
        'bigtable_info'
    ]

    for i, value in enumerate(row):
        row_key = 'test_c{}'.format(i)
        table.put(row_key, {column_name: value})
    connection.close()

def bigtable_test2():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "key.json"
    project_id = "csse433-adb-201117"
    instance_id = "project433"
    table_name = 'test'

    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    connection = happybase.Connection(instance=instance)
    table = connection.table(table_name)
    column_family_name = 'test_cf'
    column_name = '{fam}:test_c'.format(fam=column_family_name)
    print('Getting a single greeting by row key.')
    key = 'test_c0'.encode('utf-8')
    row = table.row(key)
    print('\t{}: {}'.format(key, row[column_name.encode('utf-8')]))
    connection.close()

def bigtable_test3():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "key.json"
    project_id = "csse433-adb-201117"
    instance_id = "project433"
    table_name = 'test'

    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    connection = happybase.Connection(instance=instance)
    print('Deleting the {} table.'.format(table_name))
    connection.delete_table(table_name)

    connection.close()











###############main############################
dict = {'test':dataflow_test,
        'flushall': cache_flushall}

while True:
    inp = input('>')
    if inp=='quit':
        break
    ls= list(map(lambda x:x, reader([inp])))[0]
    func = dict.get(ls[0])
    if func is None:
        continue
    func(ls[1:])