import pickle

from   bigtable_helper import *
from  bigtable_remote import *
from  redis_lib import *

import neo4j_lib

pickle.HIGHEST_PROTOCOL = 2
from rq import Queue
import datetime


queue_conn = redis.StrictRedis(host='433-19.csse.rose-hulman.edu', port=6379, db=0)
q = Queue(connection=queue_conn)


def clear():
    job = q.enqueue(remote_clear)
    while job.result is None:
        continue


def reset():
    job = q.enqueue(remote_reset)
    while job.result is None:
        continue


@connect
def display_users(conn):
    print(conn.tables())
    if not has_table(user_table,conn):
        print("User table does not exist")
        return
    table = conn.table(user_table)
    print('Enter Username')
    user = input()
    row = table.row(user.encode('utf-8'))
    if not has_row(user, user_table, conn):
        print("Username not in database")
        return
    print("Username:", end=' ')
    print((row[b'Key:user']))
    print("Name:", end=' ')
    print((row[b'Bio:lName'] + ", ".encode('utf-8') + row[b'Bio:fName']))
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


@connect
def add_user(conn):
    print('Enter username')
    user = input()
    if has_row(user, user_table, conn):
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
    job =q.enqueue(create_user, user, f_name, l_name, email)
    while job.result is None:
        continue


@connect
def add_transaction(conn):
    print('Enter Transaction Id')
    tid = input()
    if has_row(tid, transaction_table,conn):
        print("Transaction ID already in database")
        return
    if tid == "":
        print("Must enter Transaction Id")
        return
    print('Enter username of buyer')
    buyer = input()
    if not has_row(buyer, user_table,conn):
        print("Buyer does not exist in database")
        return
    print('Enter username of seller')
    seller = input()
    if not has_row(seller, user_table,conn):
        print("Seller does not exist in database")
        return
    if buyer == seller:
        print("Buyer and seller cannot be the same")
        return
    print('Enter Product ID')
    pid = input()
    # TODO Add in caveat of player owning product
    if not has_row(pid, product_table,conn):
        print("Product does not exist in database")
        return

    job =  q.enqueue(create_transaction, tid, buyer, seller, pid)
    while job.result is None:
        continue
    if write_transactions(buyer,seller,tid+"|"+buyer+"|"+seller+"|"+pid+"|"+str(datetime.datetime.now()))==1:
        print("Cannot cache transaction right now")


@connect
def display_transaction(conn):
    if not has_table(transaction_table,conn):
        print("Transaction table does not exist")
        return
    table = conn.table(transaction_table)
    print('Enter Transaction ID')
    tid = input()
    row = table.row(tid.encode('utf-8'))
    if not has_row(tid, transaction_table,conn):
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


@connect
def add_product(conn):
    print('Enter Product ID')
    pid = input()
    if has_row(pid, product_table,conn):
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
            if not has_row(tag, tag_table,conn):
                print("Tag does not exist in database")
            else:
                tags.append(tag)
    print('Enter price')
    price = input()
    if price == "":
        print("Must enter a price")
        return
    job = q.enqueue(create_product, pid, name, desc, tags, price)
    while job.result is None:
        continue
    job = q.enqueue(neo4j_lib.add_product, pid, name, desc, tags, price)
    while job.result is None:
        continue
    if job.result ==1:
        print("Product is not saved in Neo4j")
    if job.result ==2:
        print("At least one tag is not mapped to Product in Neo4j")



@connect
def display_product(conn):
    if not has_table(product_table,conn):
        print("Product table does not exist")
        return
    table = conn.table(product_table)
    print('Enter Product ID')
    pid = input()
    row = table.row(pid.encode('utf-8'))
    if not has_row(pid, product_table,conn):
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

@connect
def add_ride(conn):
    print('Enter Ride ID')
    rid = input()
    if has_row(rid, ride_table,conn):
        print("Ride ID already in database")
        return
    if rid == "":
        print("Must enter a Ride ID")
        return
    print('Enter username of driver')
    driver = input()
    if not has_row(driver, user_table,conn):
        print("Driver does not exist in database")
        return
    print('Enter username of rider')
    rider = input()
    if not has_row(rider, user_table,conn):
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
    job= q.enqueue(create_ride, rid, driver, rider, dest, miles, price)
    while job.result is None:
        continue


@connect
def display_ride(conn):
    if not has_table(ride_table,conn):
        print("Ride table does not exist")
        return
    table = conn.table(ride_table)
    print('Enter Ride ID')
    rid = input()
    row = table.row(rid.encode('utf-8'))
    if not has_row(rid, ride_table,conn):
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

@connect
def add_review(conn):
    print('Enter Review ID')
    rvid = input()
    if has_row(rvid, review_table,conn):
        print("Review ID already in database")
        return
    if rvid == "":
        print("Must enter a Review ID")
        return
    print('Enter username of Reviewer')
    patron = input()
    if not has_row(patron, user_table,conn):
        print("Reviewer does not exist in database")
        return
    print('Enter username of the reviewed')
    provider = input()
    if not has_row(provider, user_table,conn):
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
    job = q.enqueue(create_review, rvid, patron, provider, contents)
    while job.result is None:
        continue


@connect
def display_review(conn):
    if not has_table(review_table,conn):
        print("Review table does not exist")
        return
    table = conn.table(review_table)
    print('Enter Review ID')
    rvid = input()
    row = table.row(rvid.encode('utf-8'))
    if not has_row(rvid, review_table,conn):
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

@connect
def add_tag(conn):
    print('Enter Tag ID')
    tgid = input()
    if has_row(tgid, tag_table,conn):
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
    job = q.enqueue(create_tag, tgid, name)
    while job.result is None:
        continue

    job = q.enqueue(neo4j_lib.add_tag, tgid, name)
    while job.result is None:
        continue
    if job.result ==1:
        print("Tag is not saved in Neo4j")



@connect
def display_tag(conn):
    if not has_table(tag_table,conn):
        print("Tag table does not exist")
        return
    table = conn.table(tag_table)
    print('Enter Tag ID')
    tgid = input()
    row = table.row(tgid.encode('utf-8'))
    if not has_row(tgid, tag_table,conn):
        print("Tag ID not in database")
        return
    print("Tag ID:", end=' ')
    print((row[b'Key:TGID']))
    print("Tag Name:", end=' ')
    print((row[b'Info:name']))


@connect
def addReviewToUser(connection, userName, rid):
    if(not hasRow(userName, user_table)):
        print("User does not exist in database")
        return
    if(not hasRow(rid, review_table)):
        print("Review does not exist in database")
        return
    uTable = connection.table(user_table)
    uRow = uTable.row(userName)
    userReviews = convertStringToArray(uRow[b'Transactions:reviews'])
    userReviews.append(rid)
    stringReviews = convertArrayToString(userReviews)
    job = q.enqueue(update_user_reviews, userName, stringReviews)
    while job.result is None:
        continue

@connect
def createRide(connection, rid, driver, rider, dest, miles, price):
    table = connection.table(ride_table)
    job = q.enqueue(table.put(rid, {b'Key:RID': rid,
        b'Users:driver': driver, b'Users:rider': rider,
        b'Info:destination': dest, b'Info:mileage': miles, b'Info:price': price}))
    while job.result is None:
        continue


@connect
def addRideToUsers(connection, driver, rider, rid):
    if(not hasRow(driver, user_table)):
        print("Driver does not exist in database")
        return
    if(not hasRow(rider, user_table)):
        print("Rider does not exist in database")
        return
    if(not hasRow(rid, rideTable)):
        print("Ride does not exist in database")
        return
    uTable = connection.table(user_table)
    driverRow = uTable.row(driver)
    riderRow = uTable.row(rider)
    driverDrives = convertStringToArray(driverRow[b'Transactions:rHistory'])
    riderDrives = convertStringToArray(riderRow[b'Transactions:rHistory'])
    driverDrives.append(rid)
    riderDrives.append(rid)
    stringDriverHistory = convertArrayToString(driverDrives)
    stringRiderHistory = convertArrayToString(riderDrives)
    job = q.enqueue(add_ride_to_users, driver, rider, stringDriverHistory, stringRiderHistory)
    while job.result is None:
        continue


@connect
def edit_product_name(connection, productTable,pid,name):
    table = connection.table(productTable)
    table.put(pid, {b'Info:name': name})

@connect
def edit_product_desc(connection, productTable,pid,desc):
    table = connection.table(productTable)
    table.put(pid, {b'Info:name': desc})

@connect
def edit_product_price(connection, productTable,pid,price):
    table = connection.table(productTable)
    table.put(pid, {b'Info:name': price})

@connect
def deleteProduct(connection,tablel, pid):
    table = connection.table(tablel)
    table.delete(pid)

@connect
def put_tag(con,tablel,pid, tag):
    table = con.table(tablel)
    table.put(pid, {b'Tags:tags': tag})
