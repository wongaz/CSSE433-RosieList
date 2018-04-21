import redis
from csv import reader
import pickle
pickle.HIGHEST_PROTOCOL = 2
from rq import Queue
from time import sleep
from bigtable_funcs import *

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