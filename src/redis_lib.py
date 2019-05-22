import redis
from functools import wraps


def redis_connect(func):
    @wraps(func)
    def function_wrapper(*args, **kwargs):
        cache_conn = redis.StrictRedis(host='433-17.csse.rose-hulman.edu', port=6379, db=0)
        return func(cache_conn, *args, **kwargs)
    return function_wrapper


@redis_connect
def write_transactions(conn,buyer,seller,tr):
    if conn.lpush('log:'+buyer,tr) == 0:
        return 1
    if conn.lpush('log:'+seller, tr)==0:
        return 1
    return 0


@redis_connect
def read_transactions(conn,user):
    return conn.lrange('log:'+user,0,-1)


def get_trans_from_cache():
    user = input("What user's transactions do you want to view?")
    print(read_transactions(user))


@redis_connect
def cache_flushall(conn):
    return conn.flushall()