
from bigtable_helper import *
import pickle

import redis

pickle.HIGHEST_PROTOCOL = 2
from rq import Queue
userTable = 'Rosie-List-Users'
transactionTable = 'Rosie-List-Transactions'
rideTable = 'Rosie-List-Rides'
reviewTable = 'Rosie-List-Reviews'
productTable = 'Rosie-List-Products'
tagTable = 'Rosie-List-Tags'

queue_conn = redis.StrictRedis(host='433-19.csse.rose-hulman.edu', port=6379, db=0)
q = Queue(connection=queue_conn)

def convertArrayToString(array):
    result = ""
    for data in array:
        result = result + "|" + data
    return result[1:]

def convertStringToArray(inputString):
    temp = inputString.decode('utf-8')
    if(temp == ""):
        return []
    result = temp.split("|")
    return result

@connect
def hasTable(connection, name_of_table):
    tableList = connection.tables()
    return (name_of_table in tableList)

@connect
def hasRow(connection, name_of_row, name_of_table):
    table = connection.table(name_of_table)
    row = table.row(name_of_row.encode('UTF-8'))
    return any(row)

def printArray(attribute, entities):
    if(len(entities) == 0):
        print(attribute + ": None".encode('utf-8'))
    else:
        print(attribute + ":".encode('utf-8'),)
        for data in entities:
            print(data + ",".encode('utf-8'),)
        print("")


@connect
def printProductArray(connection,pidList):
    pTable = connection.table(productTable)
    attribute = "Products offered"
    if(len(pidList) == 0):
        print(attribute + ": None")
    else:
        print(attribute + ":",)
        for pid in pidList:
            pRow = pTable.row(pid.encode('utf-8'))
            print(pRow[b'Info:name'] + "(PID:".encode("utf-8") + pid.encode("utf-8") + ")".encode("utf-8") + ",".encode("utf-8"))
        print("")

@connect
def printReviewArray(connection,rvidList):
    rTable = connection.table(reviewTable)
    uTable = connection.table(userTable)
    attribute = "Reviews by"
    if(len(rvidList) == 0):
        print(attribute + ": None")
    else:
        print(attribute + ":",)
        for rvid in rvidList:
            rRow = rTable.row(rvid.encode('utf-8'))
            reviewerId = rRow[b'Users:reviewer']
            uRow = uTable.row(reviewerId)
            print("[".encode("utf-8") + (uRow[b'Bio:fName']) + " ".encode("utf-8") + uRow[b'Bio:lName'] + "(Reveiw ID:".encode("utf-8") + rvid.encode("utf-8") + ")".encode("utf-8") + "]".encode("utf-8") + ",".encode("utf-8"))
        print("")

@connect
def printTransactionArray(connection, transactionList, userId):
    tTable = connection.table(transactionTable)
    uTable = connection.table(userTable)
    pTable = connection.table(productTable)
    attribute = "Transactions"
    if(len(transactionList) == 0):
        print(attribute + ": None")
    else:
        print(attribute + "-")
        for tid in transactionList:
            tRow = tTable.row(tid.encode('utf-8'))
            buyerId = tRow[b'Users:buyer']
            sellerId = tRow[b'Users:seller']
            pid = tRow[b'Product:PID']

            pRow = pTable.row(pid)
            buyerRow = uTable.row(buyerId)
            sellerRow = uTable.row(sellerId)

            state = ""
            otherState = ""

            if buyerId == userId:
                state = "User was Buyer, "
                otherState = "Seller: ".encode("utf-8") + sellerRow[b'Bio:fName'] + " ".encode("utf-8") + sellerRow[b'Bio:lName']

            if sellerId == userId:
                state = "User was Seller, "
                otherState = "Buyer: ".encode("utf-8") + buyerRow[b'Bio:fName'] + " ".encode("utf-8") + buyerRow[b'Bio:lName']

            productName = pRow[b'Info:name']

            print("[".encode("utf-8") + state.encode("utf-8") + " ".encode("utf-8") + otherState.encode("utf-8") + ", Product Name: ".encode("utf-8") + productName + ", Product ID: ".encode("utf-8") + pid + ", Transaction ID: ".encode("utf-8") + tid.encode('utf-8') + "]".encode("utf-8"))
        print("")

@connect
def printRideArray(connection,rideList, userId):
    rTable = connection.table(rideTable)
    uTable = connection.table(userTable)
    pTable = connection.table(productTable)
    attribute = "Rides"
    if(len(rideList) == 0):
        print(attribute + ": None")
    else:
        print(attribute + "-")
        for rid in rideList:
            rRow = rTable.row(rid.encode('utf-8'))
            driverId = rRow[b'Users:driver']
            riderId = rRow[b'Users:rider']
            destination = rRow[b'Info:destination']

            driverRow = uTable.row(driverId)
            riderRow = uTable.row(riderId)

            state = ""
            otherState = ""

            if driverId == userId:
                state = "User was Driver, "
                otherState = "Rider: ".encode("utf-8") + riderRow[b'Bio:fName'] + " ".encode("utf-8") + riderRow[b'Bio:lName']

            if riderId == userId:
                state = "User was Rider, "
                otherState = "Driver: ".encode("utf-8") + driverRow[b'Bio:fName'] + " ".encode("utf-8") + driverRow[b'Bio:lName']

            print("[".encode("utf-8") + state.encode("utf-8") + " ".encode("utf-8") + otherState.encode("utf-8") + ", Destination: ".encode("utf-8") + destination + ", Ride ID: ".encode("utf-8") + rid.encode("utf-8") + "]".encode("utf-8"))
        print("")


def addUser():   
    print('Enter username')
    user = input()
    if(hasRow(user, userTable)):
        print("Username already in database")
        return
    if(user == ""):
        print("Must enter a username")
        return
    print('Enter First Name')
    fName = input()
    if(fName == ""):
        print("Must enter a first name")
        return
    print('Enter Last Name')
    lName = input()
    if(lName == ""):
        print("Must enter a last name")
        return
    print('Enter Email')
    email = input()
    if(email == ""):
        print("Must enter an email")
        return
    q.enqueue(createUser,user, fName, lName, email)

@connect
def createUser(connection,user, fName, lName, email):
    table = connection.table(userTable)
    table.put(user, {b'Key:user': user, 
        b'Bio:fName': fName, b'Bio:lName': lName, b'Bio:email': email,
        b'Transactions:tHistory': "", b'Transactions:rHistory': "", 
        b'Transactions:products': "", b'Transactions:reviews': ""})

@connect
def createTransaction(connection,tid, buyer, seller, pid):
    table = connection.table(transactionTable)
    table.put(tid, {b'Key:TID': tid, 
        b'Users:buyer': buyer, b'Users:seller': seller, 
        b'Product:PID': pid})


@connect
def createProduct(connection, pid, name, desc, tags, price) :
    table = connection.table(productTable)
    stringTags = convertArrayToString(tags)
    table.put(pid, {b'Key:PID': pid, 
        b'Info:name': name, b'Info:description': desc, b'Info:price': price,
        b'Tags:tags': stringTags})


@connect
def printTagArray(connection,tagList):
    tTable = connection.table(tagTable)
    attribute = "Tags"
    if(len(tagList) == 0):
        print(attribute + ": None")
    else:
        print(attribute + ":".encode('utf-8'),)
        for tgid in tagList:
            tRow = tTable.row(tgid.encode('utf-8'))
            print(tRow[b'Info:name']) + "(Tag ID:".encode("utf-8") + tgid + ")".encode("utf-8") + ",".encode("utf-8"),
        print("")

@connect
def createTag(connection,tgid, name):
    table = connection.table(tagTable)
    table.put(tgid, {b'Key:TGID': tgid, 
        b'Info:name': name})



@connect
def addProductToUser(connection,userName, pid):
    if(not hasRow(userName, userTable)):
        print("User does not exist in database")
        return   
    if(not hasRow(pid, productTable)):
        print("Product does not exist in database")
        return
    uTable = connection.table(userTable)
    uRow = uTable.row(userName.encode('utf-8'))
    userProducts = convertStringToArray(uRow[b'Transactions:products'])
    userProducts.append(pid)
    stringProducts = convertArrayToString(userProducts)
    uTable.put(userName, {b'Transactions:products': stringProducts})


@connect
def addTransactionToUsers(connection,buyer, seller, tid):
    if(not hasRow(buyer, userTable)):
        print("User does not exist in database")
        return   
    if(not hasRow(seller, userTable)):
        print("User does not exist in database")
        return 
    if(not hasRow(tid, transactionTable)):
        print("Review does not exist in database")
        return
    uTable = connection.table(userTable)
    buyerRow = uTable.row(buyer.encode('utf-8'))
    sellerRow = uTable.row(seller.encode('utf-8'))
    buyerHistory = convertStringToArray(buyerRow[b'Transactions:tHistory'])
    sellerHistory = convertStringToArray(sellerRow[b'Transactions:tHistory'])
    buyerHistory.append(tid)
    sellerHistory.append(tid)
    stringBuyerHistory = convertArrayToString(buyerHistory)
    stringSellerHistory = convertArrayToString(sellerHistory)
    uTable.put(buyer, {b'Transactions:tHistory': stringBuyerHistory})
    uTable.put(seller, {b'Transactions:tHistory': stringSellerHistory})

@connect
def userHasProduct(connection,user, pid):
    table = connection.table(userTable)
    row = table.row(user.encode('utf-8'))
    productList = convertStringToArray(row[b'Transactions:products'])
    if pid in productList:
        return True
    else:
        return False

@connect
def removeProductFromUser(connection,user, pid):
    table = connection.table(userTable)
    row = table.row(user.encode('utf-8'))
    products = convertStringToArray(row[b'Transactions:products'])
    if pid in products:
        products.remove(pid)
    table.put(user, {b'Transactions:products': convertArrayToString(products)})

@connect
def productInTransaction(connection,pid):
    table = connection.table(transactionTable)
    transactions = []
    for key, data in table.scan():
        for key, data in table.scan():
            productList = convertStringToArray(data[b'Product:PID'])
            if(pid in productList):
                transactions.append(key)
    return transactions

@connect
def productInUser(connection,pid):
    table = connection.table(userTable)
    users = []
    for key, data in table.scan():
        productList = convertStringToArray(data[b'Transactions:products'])
        if(pid in productList):
            users.append(key)
    return users



