
from bigtable_helper import *
userTable = 'Rosie-List-Users'
transactionTable = 'Rosie-List-Transactions'
rideTable = 'Rosie-List-Rides'
reviewTable = 'Rosie-List-Reviews'
productTable = 'Rosie-List-Products'
tagTable = 'Rosie-List-Tags'

def convertArrayToString(array):
    result = ""
    for data in array:
        result = result + "|" + data
    return result[1:]

def convertStringToArray(inputString):
    if(inputString == ""):
        return []
    result = inputString.split("|")
    return result

@connect
def hasTable(connection, name_of_table):
    tableList = connection.tables()
    return (name_of_table in tableList)

@connect
def hasRow(connection, name_of_row, name_of_table):
    table = connection.table(name_of_table)
    row = table.row(name_of_row)
    return any(row)

def printArray(attribute, entities):
    if(len(entities) == 0):
        print(attribute + ": None")
    else:
        print(attribute + ":",)
        for data in entities:
            print(data + ",",)
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
            pRow = pTable.row(pid)
            print (pRow[b'Info:name']) + "(PID:" + pid + ")" + ",",
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
            rRow = rTable.row(rvid)
            reviewerId = rRow[b'Users:reviewer']
            uRow = uTable.row(reviewerId)
            print("[" + (uRow[b'Bio:fName']) + " " + uRow[b'Bio:lName'] + "(Reveiw ID:" + rvid + ")" + "]" + ",")
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
            tRow = tTable.row(tid)
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
                otherState = "Seller: " + sellerRow[b'Bio:fName'] + " " + sellerRow[b'Bio:lName']

            if sellerId == userId:
                state = "User was Seller, "
                otherState = "Buyer: " + buyerRow[b'Bio:fName'] + " " + buyerRow[b'Bio:lName']

            productName = pRow[b'Info:name']

            print("[" + state + " " + otherState + ", Product Name: " + productName + ", Product ID: " + pid + ", Transaction ID: " + tid + "]")
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
            rRow = rTable.row(rid)
            driverId = rRow[b'Users:driver']
            riderId = rRow[b'Users:rider']
            destination = rRow[b'Info:destination']

            driverRow = uTable.row(driverId)
            riderRow = uTable.row(riderId)

            state = ""
            otherState = ""

            if driverId == userId:
                state = "User was Driver, "
                otherState = "Rider: " + riderRow[b'Bio:fName'] + " " + riderRow[b'Bio:lName']

            if riderId == userId:
                state = "User was Rider, "
                otherState = "Driver: " + driverRow[b'Bio:fName'] + " " + driverRow[b'Bio:lName']

            print("[" + state + " " + otherState + ", Destination: " + destination + ", Ride ID: " + rid + "]")
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
    createUser(user, fName, lName, email)

@connect
def createUser(connection,user, fName, lName, email):
    table = connection.table(userTable)
    table.put(user, {b'Key:user': user, 
        b'Bio:fName': fName, b'Bio:lName': lName, b'Bio:email': email,
        b'Transactions:tHistory': "", b'Transactions:rHistory': "", 
        b'Transactions:products': "", b'Transactions:reviews': ""})

def addTransaction():   
    print('Enter Transaction Id')
    tid = input()
    if(hasRow(tid, transactionTable)):
        print("Transaction ID already in database")
        return
    if(tid == ""):
        print("Must enter Transaction Id")
        return
    print('Enter username of buyer')
    buyer = input()
    if(not hasRow(buyer, userTable)):
        print("Buyer does not exist in database")
        return
    print('Enter username of seller')
    seller = input()
    if(not hasRow(seller, userTable)):
        print("Seller does not exist in database")
        return
    if(buyer == seller):
        print("Buyer and seller cannot be the same")
        return
    print('Enter Product ID')
    pid = input()
    if(not userHasProduct(seller, pid)):
        print("Seller does not posses product")
        return
    createTransaction(tid, buyer, seller, pid)
    addTransactionToUsers(buyer, seller, tid)
@connect
def createTransaction(connection,tid, buyer, seller, pid):
    table = connection.table(transactionTable)
    table.put(tid, {b'Key:TID': tid, 
        b'Users:buyer': buyer, b'Users:seller': seller, 
        b'Product:PID': pid})


def addProduct():
    print('Enter username of product seller')
    user = input()
    if(not hasRow(user, userTable)):
        print("User does not exist in database")
        return   
    print('Enter Product ID')
    pid = input()
    if(hasRow(pid, productTable)):
        print("Product ID already in database")
        return
    if(pid == ""):
        print("Must enter Product ID")
        return
    print('Enter name of product')
    name = input()
    if(name == ""):
        print("Must enter product name")
        return
    print('Enter description of product')
    desc = input()
    if(desc == ""):
        print("Must enter a description of product")
        return
    finishedTags = 0
    tags = []
    while (finishedTags != 1):
        print("Enter a Tag (Leave blank to stop):")
        tag = input()
        if(tag == ""):
            finishedTags = 1
        else:
            if(not hasRow(tag, tagTable)):
                print("Tag does not exist in database")
            else:
                tags.append(tag)
    print('Enter price')
    price = input()
    if(price == ""):
        print("Must enter a price")
        return
    createProduct(pid, name, desc, tags, price)
    addProductToUser(user, pid) 

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
        print(attribute + ":",)
        for tgid in tagList:
            tRow = tTable.row(tgid)
            print(tRow[b'Info:name']) + "(Tag ID:" + tgid + ")" + ",",
        print("")

def addTag():   
    print('Enter Tag ID')
    tgid = input()
    if(hasRow(tgid, tagTable)):
        print("Tag ID already in database")
        return
    if(tgid == ""):
        print("Must enter a Tag ID")
        return
    print('Enter name of tag')
    name = input()
    if(name == ""):
        print("Must enter name for tag")
        return
    createTag(tgid, name)

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
    uRow = uTable.row(userName)
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
    buyerRow = uTable.row(buyer)
    sellerRow = uTable.row(seller)
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
    row = table.row(user)
    productList = convertStringToArray(row[b'Transactions:products'])
    if pid in productList:
        return True
    else:
        return False

@connect
def removeProductFromUser(connection,user, pid):
    table = connection.table(userTable)
    row = table.row(user)
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



