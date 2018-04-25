import happybase
#Curtis Local machine
connection = happybase.Connection('dhcp-137-112-104-218.rose-hulman.edu', 9090) 
#Our Virtual Machine
#connection = happybase.Connection('433-19.csse.rose-hulman.edu', 42970) 
connection.open()

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
        print attribute + ": None"
    else:
        print attribute + ":",
        for data in entities:
            print data + ",",
        print ""
        

def displayUsers():
    print(connection.tables())
    if(not hasTable(userTable)):
        print "User table does not exist"
        return
    table = connection.table(userTable)
    print 'Enter Username'
    user = raw_input()
    row = table.row(user)
    if(not hasRow(user, userTable)):
        print "Username not in database"
        return
    print "Username:",
    print(row[b'Key:user'])
    print "Name:",
    print(row[b'Bio:lName'] + ", " + row[b'Bio:fName']) 
    print "Email:",
    print(row[b'Bio:email'])
    tHistory = convertStringToArray(row[b'Transactions:tHistory'])
    rHistory = convertStringToArray(row[b'Transactions:rHistory'])
    products = convertStringToArray(row[b'Transactions:products'])
    reviews = convertStringToArray(row[b'Transactions:reviews'])
    printArray("Transaction History", tHistory)
    printArray("Rides History", rHistory)
    printArray("Products Offered", products)
    printArray("Reviews", reviews)


def addUser():   
    print 'Enter username'
    user = raw_input()
    if(hasRow(user, userTable)):
        print "Username already in database"
        return
    if(user == ""):
        print "Must enter a username"
        return
    print 'Enter First Name'
    fName = raw_input()
    if(fName == ""):
        print "Must enter a first name"
        return
    print 'Enter Last Name'
    lName = raw_input()
    if(lName == ""):
        print "Must enter a last name"
        return
    print 'Enter Email'
    email = raw_input()
    if(email == ""):
        print "Must enter an email"
        return
    createUser(user, fName, lName, email)

def createUser(user, fName, lName, email):
    table = connection.table(userTable)
    table.put(user, {b'Key:user': user, 
        b'Bio:fName': fName, b'Bio:lName': lName, b'Bio:email': email,
        b'Transactions:tHistory': "", b'Transactions:rHistory': "", 
        b'Transactions:products': "", b'Transactions:reviews': ""})

def addTransaction():   
    print 'Enter Transaction Id'
    tid = raw_input()
    if(hasRow(tid, transactionTable)):
        print "Transaction ID already in database"
        return
    if(tid == ""):
        print "Must enter Transaction Id"
        return
    print 'Enter username of buyer'
    buyer = raw_input()
    if(not hasRow(buyer, userTable)):
        print "Buyer does not exist in database"
        return
    print 'Enter username of seller'
    seller = raw_input()
    if(not hasRow(seller, userTable)):
        print "Seller does not exist in database"
        return
    if(buyer == seller):
        print "Buyer and seller cannot be the same"
        return
    print 'Enter Product ID'
    pid = raw_input()
    #TODO Add in caveat of player owning product
    if(not hasRow(pid, productTable)):
        print "Product does not exist in database"
        return
    createTransaction(tid, buyer, seller, pid) 

def createTransaction(tid, buyer, seller, pid):
    table = connection.table(transactionTable)
    table.put(tid, {b'Key:TID': tid, 
        b'Users:buyer': buyer, b'Users:seller': seller, 
        b'Product:PID': pid})

def displayTransaction():
    if(not hasTable(transactionTable)):
        print "Transaction table does not exist"
        return
    table = connection.table(transactionTable)
    print 'Enter Transaction ID'
    tid = raw_input()
    row = table.row(tid)
    if(not hasRow(tid, transactionTable)):
        print "Transaction ID not in database"
        return
    print "Transaction ID:",
    print(row[b'Key:TID'])
    print "Buyer Username:",
    print(row[b'Users:buyer'])
    print "Seller Username:",
    print(row[b'Users:seller']) 
    print "Product ID:",
    print(row[b'Product:PID'])

#Not sure if right format
def createTransactionOldFormat(TID, buyer, seller, price, product, tags):
    table = connection.table(transactionTable)
    table.put(user, {b'Key:TID': TID, 
        b'Users:buyer': buyer, b'Users:seller': seller, 
        b'Product:price': price, b'Product:pName': product, b'Product:tags': tags})

def addProduct():   
    print 'Enter Product ID'
    pid = raw_input()
    if(hasRow(pid, productTable)):
        print "Product ID already in database"
        return
    if(pid == ""):
        print "Must enter Product ID"
        return
    print 'Enter name of product'
    name = raw_input()
    if(name == ""):
        print "Must enter product name"
        return
    print 'Enter description of product'
    desc = raw_input()
    if(desc == ""):
        print "Must enter a description of product"
        return
    finishedTags = 0
    tags = []
    while (finishedTags != 1):
        print "Enter a Tag (Leave blank to stop):"
        tag = raw_input()
        if(tag == ""):
            finishedTags = 1
        else:
            if(not hasRow(tag, tagTable)):
                print "Tag does not exist in database"
            else:
                tags.append(tag) 
    print 'Enter price'
    price = raw_input()
    if(price == ""):
        print "Must enter a price"
        return
    createProduct(pid, name, desc, tags, price) 

def createProduct(pid, name, desc, tags, price) :
    table = connection.table(productTable)
    stringTags = convertArrayToString(tags)
    table.put(pid, {b'Key:PID': pid, 
        b'Info:name': name, b'Info:description': desc, b'Info:price': price,
        b'Tags:tags': stringTags})

def displayProduct():
    if(not hasTable(productTable)):
        print "Product table does not exist"
        return
    table = connection.table(productTable)
    print 'Enter Product ID'
    pid = raw_input()
    row = table.row(pid)
    if(not hasRow(pid, productTable)):
        print "Product ID not in database"
        return
    print "Product ID:",
    print(row[b'Key:PID'])
    print "Product Name:",
    print(row[b'Info:name'])
    print "Description:",
    print(row[b'Info:description'])
    print "Price:",
    print(row[b'Info:price'])
    tags = convertStringToArray(row[b'Tags:tags'])
    printArray("Tags", tags)

def addRide():   
    print 'Enter Ride ID'
    rid = raw_input()
    if(hasRow(rid, rideTable)):
        print "Ride ID already in database"
        return
    if(rid == ""):
        print "Must enter a Ride ID"
        return
    print 'Enter username of driver'
    driver = raw_input()
    if(not hasRow(driver, userTable)):
        print "Driver does not exist in database"
        return
    print 'Enter username of rider'
    rider = raw_input()
    if(not hasRow(rider, userTable)):
        print "Rider does not exist in database"
        return
    if(driver == rider):
        print "Driver and rider cannot be the same"
        return
    print 'Enter destination of ride'
    dest = raw_input()
    if(dest == ""):
        print "Must enter a destination for ride"
        return
    print 'Enter the mileage of ride'
    miles = raw_input()
    if(miles == ""):
        print "Must enter a mileage for ride"
        return
    print 'Enter the price of the ride'
    price = raw_input()
    if(price == ""):
        print "Must enter a price for the ride"
        return
    createRide(rid, driver, rider, dest, miles, price)

def createRide(rid, driver, rider, dest, miles, price):
    table = connection.table(rideTable)
    table.put(rid, {b'Key:RID': rid, 
        b'Users:driver': driver, b'Users:rider': rider,
        b'Info:destination': dest, b'Info:mileage': miles, b'Info:price': price})

def displayRide():
    if(not hasTable(rideTable)):
        print "Ride table does not exist"
        return
    table = connection.table(rideTable)
    print 'Enter Ride ID'
    rid = raw_input()
    row = table.row(rid)
    if(not hasRow(rid, rideTable)):
        print "Ride ID not in database"
        return
    print "Ride ID:",
    print(row[b'Key:RID'])
    print "Rider Username:",
    print(row[b'Users:rider'])
    print "Driver Username:",
    print(row[b'Users:driver']) 
    print "Destination:",
    print(row[b'Info:destination'])
    print "Mileage:",
    print(row[b'Info:mileage'])
    print "Price:",
    print(row[b'Info:price'])

def addReview():   
    print 'Enter Review ID'
    rvid = raw_input()
    if(hasRow(rvid, reviewTable)):
        print "Review ID already in database"
        return
    if(rvid == ""):
        print "Must enter a Review ID"
        return
    print 'Enter username of Reviewer'
    patron = raw_input()
    if(not hasRow(patron, userTable)):
        print "Reviewer does not exist in database"
        return
    print 'Enter username of the reviewed'
    provider = raw_input()
    if(not hasRow(provider, userTable)):
        print "Reviewed user does not exist in database"
        return
    if(patron == provider):
        print "Reviewer and reviewed cannot be the same"
        return
    print 'Enter contents of review'
    contents = raw_input()
    if(contents == ""):
        print "Must enter contents for review"
        return
    createReview(rvid, patron, provider, contents)

def createReview(rvid, patron, provider, contents):
    table = connection.table(reviewTable)
    table.put(rvid, {b'Key:RVID': rvid, 
        b'Users:reviewer': patron, b'Users:reviewed': provider,
        b'Info:contents': contents})

def displayReview():
    if(not hasTable(reviewTable)):
        print "Review table does not exist"
        return
    table = connection.table(reviewTable)
    print 'Enter Review ID'
    rvid = raw_input()
    row = table.row(rvid)
    if(not hasRow(rvid, reviewTable)):
        print "Review ID not in database"
        return
    print "Review ID:",
    print(row[b'Key:RVID'])
    print "Reviewer Username:",
    print(row[b'Users:reviewer'])
    print "Reviewed Username:",
    print(row[b'Users:reviewed']) 
    print "Review Contents:",
    print(row[b'Info:contents'])

def addTag():   
    print 'Enter Tag ID'
    tgid = raw_input()
    if(hasRow(tgid, tagTable)):
        print "Tag ID already in database"
        return
    if(tgid == ""):
        print "Must enter a Tag ID"
        return
    print 'Enter name of tag'
    name = raw_input()
    if(name == ""):
        print "Must enter name for tag"
        return
    createTag(tgid, name)

def createTag(tgid, name):
    table = connection.table(tagTable)
    table.put(tgid, {b'Key:TGID': tgid, 
        b'Info:name': name})

def displayTag():
    if(not hasTable(tagTable)):
        print "Tag table does not exist"
        return
    table = connection.table(tagTable)
    print 'Enter Tag ID'
    tgid = raw_input()
    row = table.row(tgid)
    if(not hasRow(tgid, tagTable)):
        print "Tag ID not in database"
        return
    print "Tag ID:",
    print(row[b'Key:TGID'])
    print "Tag Name:",
    print(row[b'Info:name'])

#Main
persist = 1

userTable = 'Rosie-List-Users'
transactionTable = 'Rosie-List-Transactions'
rideTable = 'Rosie-List-Rides'
reviewTable = 'Rosie-List-Reviews'
productTable = 'Rosie-List-Products'
tagTable = 'Rosie-List-Tags'

while(persist == 1):
    print "Enter a command:"
    print "1 - Display Users"
    print "2 - Add User"
    print "3 - Display Transactions"
    print "4 - Add Transaction"
    print "5 - Display Product"
    print "6 - Add Product"
    print "7 - Display Ride"
    print "8 - Add Ride"
    print "9 - Display Review"
    print "10 - Add Review"
    print "11 - Display Tag"
    print "12 - Add Tag"
    print "C - Clear"
    print "R - Reset"
    print "q - Quit"
    command = raw_input()

    if command == '1':
        displayUsers()
        
    if command == '2':
        addUser()

    if command == '3':
        displayTransaction()
        
    if command == '4':
        addTransaction()

    if command == '5':
        displayProduct()
        
    if command == '6':
        addProduct()
    
    if command == '7':
        displayRide()
        
    if command == '8':
        addRide()

    if command == '9':
        displayReview()
        
    if command == '10':
        addReview()

    if command == '11':
        displayTag()
        
    if command == '12':
        addTag()
    
    if command == 'C':
        connection.disable_table(userTable)
        connection.delete_table(userTable)

    if command == 'R':
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
        
        

    if command == 'q':
        persist = 0
    
    
    if persist == 1:
        print "Continue (Y/n)"
        command = user = raw_input()
        if command == 'n':
            persist = 0