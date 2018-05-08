import redis

conn = redis.Redis()

def write_history(user, item):
    pageType = item[0]
    pageId = item[1:]
    if pageType == 'u':
        pageType = "User: "
    if pageType == 't':
        pageType = "Transaction: "
    if pageType == 'r':
        pageType = "Ride: "
    if conn.lpush('history:'+user, pageType + pageId) == 0:
        return 1
    return 0

def read_history(user):
    return conn.lrange('history:'+user,0,-1)

def write_transactions(buyer,seller,tr):
    if conn.lpush('log:'+buyer,tr) == 0:
        return 1
    if conn.lpush('log:'+seller, tr)==0:
        return 1
    return 0

def read_transactions(user):
    return conn.lrange('log:'+user,0,-1)


def get_trans_from_cache():
    user = input("What user's transactions do you want to view?")
    print(read_transactions(user))