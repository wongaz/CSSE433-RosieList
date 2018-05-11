from terminal_helper import *
from local_redis import *


userTable = 'Rosie-List-Users'
transactionTable = 'Rosie-List-Transactions'
rideTable = 'Rosie-List-Rides'
reviewTable = 'Rosie-List-Reviews'
productTable = 'Rosie-List-Products'
tagTable = 'Rosie-List-Tags'

def displayProductWithId(pid):
    table = connection.table(productTable)
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
    printTagArray(tags)

def searchProduct():
    print 'Enter Product Name'
    name = raw_input()
    table = connection.table(productTable)
    count = 1
    for key, data in table.scan():
        if name == data['Info:name']:
            print ""
            print "Match " + str(count)
            displayProductWithId(key)
            count = count + 1

def editProduct(user):
    table = connection.table(productTable)
    uTable = connection.table(userTable)
    print "Enter a pid"
    pid = raw_input()
    if(not hasRow(pid, productTable)):
        print "Product ID not in database"
        return
    row = uTable.row(user)
    userProducts = convertStringToArray(row[b'Transactions:products'])
    if not pid in userProducts:
        print "Product belongs to a different user, cannot be edited"
        return
    print "Select a field to edit:"
    print "1 - Name"
    print "2 - Description"
    print "3 - Price"
    command = raw_input()
    if command == '1':
        print "Enter a new name"
        name = raw_input()
        table.put(pid, {b'Info:name': name})
    if command == '2':
        print "Enter a new Description"
        desc = raw_input()
        table.put(pid, {b'Info:description': desc})
    if command == '3':
        print "Enter a new price"
        price = raw_input()
        table.put(pid, {b'Info:price': price})

def addProductWithUser(user):  
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
    addProductToUser(user, pid)

def deleteProductWithUser(user):
    table = connection.table(productTable)
    uTable = connection.table(userTable)
    print "Enter a pid"
    pid = raw_input()
    if(not hasRow(pid, productTable)):
        print "Product ID not in database"
        return
    row = uTable.row(user)
    userProducts = convertStringToArray(row[b'Transactions:products'])
    if not pid in userProducts:
        print "Product belongs to a different user, cannot be deleted"
        return
    arrayOfTransactions = productInTransaction(pid)
    if(not (len(arrayOfTransactions) == 0)):
        print "Can't delete product, used in transaction " + arrayOfTransactions[0]
        return
    sellerList = productInUser(pid)
    for seller in sellerList:
        removeProductFromUser(seller, pid)
    table.delete(pid)
    
def tagProductInUser(user):
    table = connection.table(productTable)
    uTable = connection.table(userTable)
    print "Enter a pid"
    pid = raw_input()
    if(not hasRow(pid, productTable)):
        print "Product ID not in database"
        return
    row = uTable.row(user)
    userProducts = convertStringToArray(row[b'Transactions:products'])
    if not pid in userProducts:
        print "Product belongs to a different user, cannot be tagged"
        return
    print 'Would you like to read the available tags (y/N)?'
    command = raw_input()
    if command == 'y':
        table = connection.table(tagTable)
        for key, data in table.scan():
            print "Tag: " + data['Info:name'] + "ID: " + key
    print 'Enter a Tag ID'
    tgid = raw_input()
    if(not hasRow(tgid, tagTable)):
        print "Tag ID does note exist in database"
        print "Would you like to create a new tag (Y/n)?"
        command = raw_input()
        if command == 'n':
            return
        createTagWithID(tgid)
    table = connection.table(productTable)
    row = table.row(pid)
    arrayTags = convertStringToArray(row[b'Tags:tags'])
    arrayTags.append(tgid)
    stringTags = convertArrayToString(arrayTags)
    table.put(pid, {b'Tags:tags': stringTags})
    return 

def createTagWithID(tgid):
    name = ""   
    while(name == ""):
        print 'Enter name of tag'
        name = raw_input()
        if(name == ""):
            print "Must enter name for tag"
    createTag(tgid, name)

def buyProduct(user):
    table = connection.table(productTable)
    uTable = connection.table(userTable)
    uRow = uTable.row(user)
    print "Enter a pid"
    pid = raw_input()
    row = table.row(pid)
    if(not hasRow(pid, productTable)):
        print "Product ID not in database"
        return
    userProducts = convertStringToArray(uRow[b'Transactions:products'])
    if pid in userProducts:
        print "Cannot buy product from self"
        return
    table = connection.table(productTable)
    for key, data in uTable.scan():
        userProducts = convertStringToArray(data[b'Transactions:products'])
        if pid in userProducts:
            seller = key
    tid = ""
    while tid == "":
        print 'Enter Transaction Id'
        tid = raw_input()
        if(tid == ""):
            print "Must enter Transaction Id"
        if(hasRow(tid, transactionTable)):
            print "Transaction ID already in database"
            tid = ""
    createTransaction(tid, user, seller, pid)
    addTransactionToUsers(user, seller, tid)
    write_history(seller, 't' + tid)
    write_history(user, 't' + tid)  

def productTerminal(user):
    persist = 1
    while(persist == 1):
        print "Enter a command:"
        print "1 - Print Product List"
        print "2 - Search Product By Name"
        print "3 - Add a new Product"
        print "4 - Edit Product Information"
        print "5 - Delete Product"
        print "6 - Tag Product"
        print "7 - Buy Product"
        print "q - Return to root menu"
        command = raw_input()
        
        if command == '1':
            table = connection.table(productTable)
            for key, data in table.scan():
                displayProductWithId(key)

        if command == '2':
            searchProduct()

        if command == '3':
            addProductWithUser(user)

        if command == '4':
            editProduct(user)

        if command == '5':
            deleteProductWithUser(user)

        if command == '6':
            tagProductInUser(user)

        if command == '7':
            buyProduct(user)

        if command == 'q':
            persist = 0
        
        if persist == 1:
            print "Continue (Y/n)"
            command = raw_input()
            if command == 'n':
                persist = 0