from bigtable_remote import *
import redis
import pickle

pickle.HIGHEST_PROTOCOL = 2
from rq import Queue

queue_conn = redis.StrictRedis(host='433-19.csse.rose-hulman.edu', port=6379, db=0)
q = Queue(connection=queue_conn)


def reset():
    job = q.enqueue(remote_reset)
    while job.result is None:
        continue


@connect
def display_users(conn):
    print(conn.tables())
    if not has_table(user_table):
        print("User table does not exist")
        return
    table = conn.table(user_table)
    print('Enter Username')
    user = input()
    row = table.row(user)
    if not has_row(user, user_table, conn):
        print("Username not in database")
        return
    print("Username:", end=' ')
    print((row[b'Key:user']))
    print("Name:", end=' ')
    print((row[b'Bio:lName'] + ", " + row[b'Bio:fName']))
    print("Email:", end=' ')
    print((row[b'Bio:email']))
    t_history = convert_string_to_array(row[b'Transactions:t_history'])
    r_history = convert_string_to_array(row[b'Transactions:r_history'])
    products = convert_string_to_array(row[b'Transactions:products'])
    reviews = convert_string_to_array(row[b'Transactions:reviews'])
    print_array("Transaction History", t_history)
    print_array("Rides History", r_history)
    print_array("Products Offered", products)
    print_array("Reviews", reviews)


def add_user():
    print('Enter username')
    user = input()
    if has_row(user, user_table):
        print("Username already in database")
        return
    if user == "":
        print("Must enter a username")
        return
    print('Enter First Name')
    f_name = input()
    if f_name == "":
        print("Must enter a first name")
        return
    print('Enter Last Name')
    l_name = input()
    if l_name == "":
        print("Must enter a last name")
        return
    print('Enter Email')
    email = input()
    if email == "":
        print("Must enter an email")
        return
    q.enqueue(create_user, (user, f_name, l_name, email))


def add_transaction():
    print('Enter Transaction Id')
    tid = input()
    if has_row(tid, transaction_table):
        print("Transaction ID already in database")
        return
    if tid == "":
        print("Must enter Transaction Id")
        return
    print('Enter username of buyer')
    buyer = input()
    if not has_row(buyer, user_table):
        print("Buyer does not exist in database")
        return
    print('Enter username of seller')
    seller = input()
    if not has_row(seller, user_table):
        print("Seller does not exist in database")
        return
    if buyer == seller:
        print("Buyer and seller cannot be the same")
        return
    print('Enter Product ID')
    pid = input()
    # TODO Add in caveat of player owning product
    if not has_row(pid, product_table):
        print("Product does not exist in database")
        return
    q.enqueue(create_transaction, (tid, buyer, seller, pid))


@connect
def display_transaction(conn):
    if not has_table(transaction_table):
        print("Transaction table does not exist")
        return
    table = conn.table(transaction_table)
    print('Enter Transaction ID')
    tid = input()
    row = table.row(tid)
    if not has_row(tid, transaction_table):
        print("Transaction ID not in database")
        return
    print("Transaction ID:", end=' ')
    print((row[b'Key:TID']))
    print("Buyer Username:", end=' ')
    print((row[b'Users:buyer']))
    print("Seller Username:", end=' ')
    print((row[b'Users:seller']))
    print("Product ID:", end=' ')
    print((row[b'Product:PID']))


def add_product():
    print('Enter Product ID')
    pid = input()
    if has_row(pid, product_table):
        print("Product ID already in database")
        return
    if pid == "":
        print("Must enter Product ID")
        return
    print('Enter name of product')
    name = input()
    if name == "":
        print("Must enter product name")
        return
    print('Enter description of product')
    desc = input()
    if desc == "":
        print("Must enter a description of product")
        return
    finished_tags = 0
    tags = []
    while finished_tags != 1:
        print("Enter a Tag (Leave blank to stop):")
        tag = input()
        if tag == "":
            finished_tags = 1
        else:
            if not has_row(tag, tag_table):
                print("Tag does not exist in database")
            else:
                tags.append(tag)
    print('Enter price')
    price = input()
    if price == "":
        print("Must enter a price")
        return
    q.enqueue(create_product, (pid, name, desc, tags, price))


@connect
def display_product(conn):
    if not has_table(product_table):
        print("Product table does not exist")
        return
    table = conn.table(product_table)
    print('Enter Product ID')
    pid = input()
    row = table.row(pid)
    if not has_row(pid, product_table):
        print("Product ID not in database")
        return
    print("Product ID:", end=' ')
    print((row[b'Key:PID']))
    print("Product Name:", end=' ')
    print((row[b'Info:name']))
    print("Description:", end=' ')
    print((row[b'Info:description']))
    print("Price:", end=' ')
    print((row[b'Info:price']))
    tags = convert_string_to_array(row[b'Tags:tags'])
    print_array("Tags", tags)


def add_ride():
    print('Enter Ride ID')
    rid = input()
    if has_row(rid, ride_table):
        print("Ride ID already in database")
        return
    if rid == "":
        print("Must enter a Ride ID")
        return
    print('Enter username of driver')
    driver = input()
    if not has_row(driver, user_table):
        print("Driver does not exist in database")
        return
    print('Enter username of rider')
    rider = input()
    if not has_row(rider, user_table):
        print("Rider does not exist in database")
        return
    if driver == rider:
        print("Driver and rider cannot be the same")
        return
    print('Enter destination of ride')
    dest = input()
    if dest == "":
        print("Must enter a destination for ride")
        return
    print('Enter the mileage of ride')
    miles = input()
    if miles == "":
        print("Must enter a mileage for ride")
        return
    print('Enter the price of the ride')
    price = input()
    if price == "":
        print("Must enter a price for the ride")
        return
    q.enqueue(create_ride, (rid, driver, rider, dest, miles, price))


@connect
def display_ride(conn):
    if not has_table(ride_table):
        print("Ride table does not exist")
        return
    table = conn.table(ride_table)
    print('Enter Ride ID')
    rid = input()
    row = table.row(rid)
    if not has_row(rid, ride_table):
        print("Ride ID not in database")
        return
    print("Ride ID:", end=' ')
    print((row[b'Key:RID']))
    print("Rider Username:", end=' ')
    print((row[b'Users:rider']))
    print("Driver Username:", end=' ')
    print((row[b'Users:driver']))
    print("Destination:", end=' ')
    print((row[b'Info:destination']))
    print("Mileage:", end=' ')
    print((row[b'Info:mileage']))
    print("Price:", end=' ')
    print((row[b'Info:price']))


def add_review():
    print('Enter Review ID')
    rvid = input()
    if has_row(rvid, review_table):
        print("Review ID already in database")
        return
    if rvid == "":
        print("Must enter a Review ID")
        return
    print('Enter username of Reviewer')
    patron = input()
    if not has_row(patron, user_table):
        print("Reviewer does not exist in database")
        return
    print('Enter username of the reviewed')
    provider = input()
    if not has_row(provider, user_table):
        print("Reviewed user does not exist in database")
        return
    if patron == provider:
        print("Reviewer and reviewed cannot be the same")
        return
    print('Enter contents of review')
    contents = input()
    if contents == "":
        print("Must enter contents for review")
        return
    q.enqueue(create_review, (rvid, patron, provider, contents))


@connect
def display_review(conn):
    if not has_table(review_table):
        print("Review table does not exist")
        return
    table = conn.table(review_table)
    print('Enter Review ID')
    rvid = input()
    row = table.row(rvid)
    if not has_row(rvid, review_table):
        print("Review ID not in database")
        return
    print("Review ID:", end=' ')
    print((row[b'Key:RVID']))
    print("Reviewer Username:", end=' ')
    print((row[b'Users:reviewer']))
    print("Reviewed Username:", end=' ')
    print((row[b'Users:reviewed']))
    print("Review Contents:", end=' ')
    print((row[b'Info:contents']))


def add_tag():
    print('Enter Tag ID')
    tgid = input()
    if has_row(tgid, tag_table):
        print("Tag ID already in database")
        return
    if tgid == "":
        print("Must enter a Tag ID")
        return
    print('Enter name of tag')
    name = input()
    if name == "":
        print("Must enter name for tag")
        return
    q.enqueue(create_tag, (tgid, name))


@connect
def display_tag(conn):
    if not has_table(tag_table):
        print("Tag table does not exist")
        return
    table = conn.table(tag_table)
    print('Enter Tag ID')
    tgid = input()
    row = table.row(tgid)
    if not has_row(tgid, tag_table):
        print("Tag ID not in database")
        return
    print("Tag ID:", end=' ')
    print((row[b'Key:TGID']))
    print("Tag Name:", end=' ')
    print((row[b'Info:name']))
