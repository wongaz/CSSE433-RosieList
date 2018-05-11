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

def hasTable(name_of_table):
    tableList = connection.tables()
    return (name_of_table in tableList)

def hasRow(name_of_row, name_of_table):
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
        

def displayUsers():
    print(connection.tables())
    if(not hasTable(userTable)):
        print("User table does not exist")
        return
    table = connection.table(userTable)
    print('Enter Username')
    user = input()
    row = table.row(user)
    if(not hasRow(user, userTable)):
        print("Username not in database")
        return
    print("Username:",)
    print(row[b'Key:user'])
    print("Name:",)
    print(row[b'Bio:lName'] + ", " + row[b'Bio:fName']) 
    print("Email:",)
    print(row[b'Bio:email'])
    tHistory = convertStringToArray(row[b'Transactions:tHistory'])
    rHistory = convertStringToArray(row[b'Transactions:rHistory'])
    products = convertStringToArray(row[b'Transactions:products'])
    reviews = convertStringToArray(row[b'Transactions:reviews'])
    printTransactionArray(tHistory, user)
    printRideArray(rHistory, user)
    printProductArray(products)
    printReviewArray(reviews)

def printProductArray(pidList):
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

def printReviewArray(rvidList):
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

def printTransactionArray(transactionList, userId):
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

def printRideArray(rideList, userId):
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

def createUser(user, fName, lName, email):
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

def createTransaction(tid, buyer, seller, pid):
    table = connection.table(transactionTable)
    table.put(tid, {b'Key:TID': tid, 
        b'Users:buyer': buyer, b'Users:seller': seller, 
        b'Product:PID': pid})


def displayTransaction():
    if(not hasTable(transactionTable)):
        print("Transaction table does not exist")
        return
    table = connection.table(transactionTable)
    print('Enter Transaction ID')
    tid = input()
    row = table.row(tid)
    if(not hasRow(tid, transactionTable)):
        print("Transaction ID not in database")
        return
    print("Transaction ID:",)
    print(row[b'Key:TID'])
    print("Buyer Username:",)
    print(row[b'Users:buyer'])
    print("Seller Username:",)
    print(row[b'Users:seller']) 
    print("Product ID:",)
    print(row[b'Product:PID'])


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

def createProduct(pid, name, desc, tags, price) :
    table = connection.table(productTable)
    stringTags = convertArrayToString(tags)
    table.put(pid, {b'Key:PID': pid, 
        b'Info:name': name, b'Info:description': desc, b'Info:price': price,
        b'Tags:tags': stringTags})

def displayProduct():
    if(not hasTable(productTable)):
        print("Product table does not exist")
        return
    table = connection.table(productTable)
    print('Enter Product ID')
    pid = input()
    row = table.row(pid)
    if(not hasRow(pid, productTable)):
        print("Product ID not in database")
        return
    print("Product ID:",)
    print(row[b'Key:PID'])
    print("Product Name:",)
    print(row[b'Info:name'])
    print("Description:",)
    print(row[b'Info:description'])
    print("Price:",)
    print(row[b'Info:price'])
    tags = convertStringToArray(row[b'Tags:tags'])
    printTagArray(tags)

def printTagArray(tagList):
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

def addRide():   
    print('Enter Ride ID')
    rid = input()
    if(hasRow(rid, rideTable)):
        print("Ride ID already in database")
        return
    if(rid == ""):
        print("Must enter a Ride ID")
        return
    print('Enter username of driver')
    driver = input()
    if(not hasRow(driver, userTable)):
        print("Driver does not exist in database")
        return
    print('Enter username of rider')
    rider = input()
    if(not hasRow(rider, userTable)):
        print("Rider does not exist in database")
        return
    if(driver == rider):
        print("Driver and rider cannot be the same")
        return
    print('Enter destination of ride')
    dest = input()
    if(dest == ""):
        print("Must enter a destination for ride")
        return
    print('Enter the mileage of ride')
    miles = input()
    if(miles == ""):
        print("Must enter a mileage for ride")
        return
    print('Enter the price of the ride')
    price = input()
    if(price == ""):
        print("Must enter a price for the ride")
        return
    createRide(rid, driver, rider, dest, miles, price)
    addRideToUsers(driver, rider, rid)

def addRideToUsers(driver, rider, rid):
    if(not hasRow(driver, userTable)):
        print("Driver does not exist in database")
        return   
    if(not hasRow(rider, userTable)):
        print("Rider does not exist in database")
        return 
    if(not hasRow(rid, rideTable)):
        print("Ride does not exist in database")
        return
    uTable = connection.table(userTable)
    driverRow = uTable.row(driver)
    riderRow = uTable.row(rider)
    driverDrives = convertStringToArray(driverRow[b'Transactions:rHistory'])
    riderDrives = convertStringToArray(riderRow[b'Transactions:rHistory'])
    driverDrives.append(rid)
    riderDrives.append(rid)
    stringDriverHistory = convertArrayToString(driverDrives)
    stringRiderHistory = convertArrayToString(riderDrives)
    uTable.put(driver, {b'Transactions:rHistory': stringDriverHistory})
    uTable.put(rider, {b'Transactions:rHistory': stringRiderHistory})

def createRide(rid, driver, rider, dest, miles, price):
    table = connection.table(rideTable)
    table.put(rid, {b'Key:RID': rid, 
        b'Users:driver': driver, b'Users:rider': rider,
        b'Info:destination': dest, b'Info:mileage': miles, b'Info:price': price})

def displayRide():
    if(not hasTable(rideTable)):
        print("Ride table does not exist")
        return
    table = connection.table(rideTable)
    print('Enter Ride ID')
    rid = input()
    row = table.row(rid)
    if(not hasRow(rid, rideTable)):
        print("Ride ID not in database")
        return
    print("Ride ID:",)
    print(row[b'Key:RID'])
    print("Rider Username:",)
    print(row[b'Users:rider'])
    print("Driver Username:",)
    print(row[b'Users:driver']) 
    print("Destination:",)
    print(row[b'Info:destination'])
    print("Mileage:",)
    print(row[b'Info:mileage'])
    print("Price:",)
    print(row[b'Info:price'])

def addReview():   
    print('Enter Review ID')
    rvid = input()
    if(hasRow(rvid, reviewTable)):
        print("Review ID already in database")
        return
    if(rvid == ""):
        print("Must enter a Review ID")
        return
    print('Enter username of Reviewer')
    patron = input()
    if(not hasRow(patron, userTable)):
        print("Reviewer does not exist in database")
        return
    print('Enter username of the reviewed')
    provider = input()
    if(not hasRow(provider, userTable)):
        print("Reviewed user does not exist in database")
        return
    if(patron == provider):
        print("Reviewer and reviewed cannot be the same")
        return
    print('Enter contents of review')
    contents = input()
    if(contents == ""):
        print("Must enter contents for review")
        return
    createReview(rvid, patron, provider, contents)
    addReviewToUser(provider, rvid)

def createReview(rvid, patron, provider, contents):
    table = connection.table(reviewTable)
    table.put(rvid, {b'Key:RVID': rvid, 
        b'Users:reviewer': patron, b'Users:reviewed': provider,
        b'Info:contents': contents})

def displayReview():
    if(not hasTable(reviewTable)):
        print("Review table does not exist")
        return
    table = connection.table(reviewTable)
    print('Enter Review ID')
    rvid =input()
    row = table.row(rvid)
    if(not hasRow(rvid, reviewTable)):
        print("Review ID not in database")
        return
    print("Review ID:",)
    print(row[b'Key:RVID'])
    print("Reviewer Username:",)
    print(row[b'Users:reviewer'])
    print("Reviewed Username:",)
    print(row[b'Users:reviewed']) 
    print("Review Contents:",)
    print(row[b'Info:contents'])

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


def createTag(tgid, name):
    table = connection.table(tagTable)
    table.put(tgid, {b'Key:TGID': tgid, 
        b'Info:name': name})


def displayTag():
    if(not hasTable(tagTable)):
        print("Tag table does not exist")
        return
    table = connection.table(tagTable)
    print('Enter Tag ID')
    tgid = input()
    row = table.row(tgid)
    if(not hasRow(tgid, tagTable)):
        print("Tag ID not in database")
        return
    print("Tag ID:",)
    print(row[b'Key:TGID'])
    print("Tag Name:",)
    print(row[b'Info:name'])


def resetDatabase():
    if(hasTable(userTable)):
        connection.disable_table(userTable)
        connection.delete_table(userTable)
    connection.create_table(
        userTable,
        {'Key': dict(),
        'Bio': dict(),
        'Transactions': dict(),   
        }
    )
    if(hasTable(transactionTable)):
        connection.disable_table(transactionTable)
        connection.delete_table(transactionTable)
    connection.create_table(
        transactionTable,
        {'Key': dict(),
        'Users': dict(),
        'Product': dict(),   
        }
    )
    if(hasTable(productTable)):
        connection.disable_table(productTable)
        connection.delete_table(productTable)
    connection.create_table(
        productTable,
        {'Key': dict(),
        'Info': dict(),
        'Tags': dict(),   
        }
    )
    if(hasTable(rideTable)):
        connection.disable_table(rideTable)
        connection.delete_table(rideTable)
    connection.create_table(
        rideTable,
        {'Key': dict(),
        'Users': dict(),
        'Info': dict(),   
        }
    )
    if(hasTable(reviewTable)):
        connection.disable_table(reviewTable)
        connection.delete_table(reviewTable)
    connection.create_table(
        reviewTable,
        {'Key': dict(),
        'Users': dict(),
        'Info': dict(),   
        }
    )
    if(hasTable(tagTable)):
        connection.disable_table(tagTable)
        connection.delete_table(tagTable)
    connection.create_table(
        tagTable,
        {'Key': dict(),
        'Info': dict(),   
        }
    )

def addProductToUser(userName, pid):
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

def addReviewToUser(userName, rid):
    if(not hasRow(userName, userTable)):
        print("User does not exist in database")
        return   
    if(not hasRow(rid, reviewTable)):
        print("Review does not exist in database")
        return
    uTable = connection.table(userTable)
    uRow = uTable.row(userName)
    userReviews = convertStringToArray(uRow[b'Transactions:reviews'])
    userReviews.append(rid)
    stringReviews = convertArrayToString(userReviews)
    uTable.put(userName, {b'Transactions:reviews': stringReviews})

def addTransactionToUsers(buyer, seller, tid):
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

def tagProduct():
    print('Enter a Tag ID')
    tgid = input()
    if(not hasRow(tgid, tagTable)):
        print("Tag ID does note exist in database")
        return
    print('Enter a Product ID')
    pid = input()
    if(not hasRow(pid, productTable)):
        print("PID does note exist in database")
        return
    table = connection.table(productTable)
    row = table.row(pid)
    arrayTags = convertStringToArray(row[b'Tags:tags'])
    arrayTags.append(tgid)
    stringTags = convertArrayToString(arrayTags)
    table.put(pid, {b'Tags:tags': stringTags})
    return

def userHasProduct(user, pid):
    table = connection.table(userTable)
    row = table.row(user)
    productList = convertStringToArray(row[b'Transactions:products'])
    if pid in productList:
        return True
    else:
        return False

def deleteUser():
    print('Enter a Username')
    user = input()
    table = connection.table(userTable)
    if(not hasRow(user, userTable)):
        print("Username does not exist in database")
        return
    table.delete(user)

def deleteTransaction():
    print('Enter a Transaction ID')
    tid = input()
    tTable = connection.table(transactionTable)
    
    if(not hasRow(tid, transactionTable)):
        print("Transaction ID does not exist in database")
        return
    tRow = tTable.row(tid)
    buyer = tRow['Users:buyer']
    seller = tRow['Users:seller']
    removeTransactionFromUser(buyer, tid)
    removeTransactionFromUser(seller, tid)
    tTable.delete(tid)
    
def removeTransactionFromUser(user, tid):
    table = connection.table(userTable)
    row = table.row(user)
    transactions = convertStringToArray(row[b'Transactions:tHistory'])
    transactions.remove(tid)
    table.put(user, {b'Transactions:tHistory': convertArrayToString(transactions)})

def deleteRide():
    print('Enter a Ride ID')
    rid = input()
    rTable = connection.table(rideTable)
    
    if(not hasRow(rid, rideTable)):
        print("Ride ID does not exist in database")
        return
    rRow = rTable.row(rid)
    driver = rRow['Users:driver']
    rider = rRow['Users:rider']
    removeRideFromUser(driver, rid)
    removeRideFromUser(rider, rid)
    rTable.delete(rid)


def removeRideFromUser(user, rid):
    table = connection.table(userTable)
    row = table.row(user)
    rides = convertStringToArray(row[b'Transactions:rHistory'])
    rides.remove(rid)
    table.put(user, {b'Transactions:rHistory': convertArrayToString(rides)})


def deleteProduct():
    print('Enter a Product ID')
    pid = input()
    pTable = connection.table(productTable)
    
    if(not hasRow(pid, productTable)):
        print("Product ID does not exist in database")
        return
    
    arrayOfTransactions = productInTransaction(pid)
    if(not (len(arrayOfTransactions) == 0)):
        print("Can't delete product, used in transaction " + arrayOfTransactions[0])
        return

    sellerList = productInUser(pid)
    for seller in sellerList:
        removeProductFromUser(seller, pid)
    pTable.delete(pid)
    
def removeProductFromUser(user, pid):
    table = connection.table(userTable)
    row = table.row(user)
    products = convertStringToArray(row[b'Transactions:products'])
    if pid in products:
        products.remove(pid)
    table.put(user, {b'Transactions:products': convertArrayToString(products)})

def productInTransaction(pid):
    table = connection.table(transactionTable)
    transactions = []
    for key, data in table.scan():
        for key, data in table.scan():
            productList = convertStringToArray(data[b'Product:PID'])
            if(pid in productList):
                transactions.append(key)
    return transactions

def productInUser(pid):
    table = connection.table(userTable)
    users = []
    for key, data in table.scan():
        productList = convertStringToArray(data[b'Transactions:products'])
        if(pid in productList):
            users.append(key)
    return users

def deleteTag():
    print('Enter a Tag ID')
    tgid = input()
    tgTable = connection.table(tagTable)
    
    if(not hasRow(tgid, tagTable)):
        print("Tag ID does not exist in database")
        return
    
    arrayOfProducts = tagInProduct(tgid)
    if(not (len(arrayOfProducts) == 0)):
        print("Can't delete tag, used in product " + arrayOfProducts[0])
        return
    tgTable.delete(tgid)

def tagInProduct(tgid):
    table = connection.table(productTable)
    tags = []
    for key, data in table.scan():
        tagList = convertStringToArray(data[b'Tags:tags'])
        if(tgid in tagList):
            tags.append(key)
    return tags



