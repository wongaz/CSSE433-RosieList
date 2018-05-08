from terminal_helper import *
from local_redis import *
import happybase
#Curtis Local machine
connection = happybase.Connection('dhcp-137-112-104-218.rose-hulman.edu', 9090) 
#Our Virtual Machine
#connection = happybase.Connection('433-19.csse.rose-hulman.edu', 42970) 
connection.open()

def displayUser(user):
    table = connection.table(userTable)
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
    printTransactionArray(tHistory, user)
    printRideArray(rHistory, user)
    printProductArray(products)
    printReviewArray(reviews)

def editBio(user):
    table = connection.table(userTable)
    print "Select a field to edit:"
    print "1 - Name"
    print "2 - Email"
    command = raw_input()
    if command == '1':
        print "Enter a new first name"
        fName = raw_input()
        print "Enter a new last name"
        lName = raw_input()
        table.put(user, {b'Bio:fName': fName, b'Bio:lName': lName})
    if command == '2':
        print "Enter a new email"
        email = raw_input()
        table.put(user, {b'Bio:email': email})

def listTransactions(user):
    uTable = connection.table(userTable)
    uRow = uTable.row(user)
    tHistory = convertStringToArray(uRow[b'Transactions:tHistory'])
    for tid in tHistory:
        table = connection.table(transactionTable)
        row = table.row(tid)
        print ""
        print "Transaction ID:",
        print(row[b'Key:TID'])
        print "Buyer Username:",
        print(row[b'Users:buyer'])
        print "Seller Username:",
        print(row[b'Users:seller']) 
        print "Product ID:",
        print(row[b'Product:PID'])

def deleteTransactionFromUser(user):
    table = connection.table(transactionTable)
    uTable = connection.table(userTable)
    print "Enter a tid"
    tid = raw_input()
    if(not hasRow(tid, transactionTable)):
        print "Rid eID not in database"
        return
    row = uTable.row(user)
    userTransactions = convertStringToArray(row[b'Transactions:tHistory'])
    if not tid in userTransactions:
        print "Transaction does not involve this user, cannot be deleted"
        return
    tRow = table.row(tid)
    buyer = tRow['Users:buyer']
    seller = tRow['Users:seller']
    removeTransactionFromUser(buyer, tid)
    removeTransactionFromUser(seller, tid)
    table.delete(tid)

def listRides(user):
    uTable = connection.table(userTable)
    uRow = uTable.row(user)
    rHistory = convertStringToArray(uRow[b'Transactions:rHistory'])
    table = connection.table(rideTable)
    for rid in rHistory:
        print ""
        row = table.row(rid)
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

def deleteRideFromUser(user):
    table = connection.table(rideTable)
    uTable = connection.table(userTable)
    print "Enter a rid"
    rid = raw_input()
    if(not hasRow(rid, rideTable)):
        print "Ride ID not in database"
        return
    row = uTable.row(user)
    userRides = convertStringToArray(row[b'Transactions:rHistory'])
    if not rid in userRides:
        print "Rid edoes not involve this user, cannot be deleted"
        return
    rRow = table.row(rid)
    driver = rRow['Users:driver']
    rider = rRow['Users:rider']
    removeRideFromUser(driver, rid)
    removeRideFromUser(rider, rid)
    table.delete(rid)

def listReviews(user):
    uTable = connection.table(userTable)
    uRow = uTable.row(user)
    reviews = convertStringToArray(uRow[b'Transactions:reviews'])
    table = connection.table(reviewTable)
    for rvid in reviews:
        row = table.row(rvid)
        print ""
        print "Review ID:",
        print(row[b'Key:RVID'])
        print "Reviewer Username:",
        print(row[b'Users:reviewer'])
        print "Reviewed Username:",
        print(row[b'Users:reviewed']) 
        print "Review Contents:",
        print(row[b'Info:contents'])

def personalTerminal(user):
    persist = 1
    while(persist == 1):
        print "Enter a command:"
        print "1 - Print Bio"
        print "2 - Edit Bio"
        print "3 - List Transactions"
        print "4 - Delete Transactions"
        print "5 - List Rides"
        print "6 - Delete Rides"
        print "7 - List reviews"
        print "8 - Print history"
        print "q - Return to root menu"
        command = raw_input()
        if command == '1':
            displayUser(user)

        if command == '2':
            editBio(user)

        if command == '3':
            listTransactions(user)

        if command == '4':
            deleteTransactionFromUser(user)

        if command == '5':
            listRides(user)

        if command == '6':
            deleteRideFromUser(user)

        if command == '7':
            listReviews(user)

        if command == '8':
            history = read_history(user)
            for data in history:
                print data

        if command == 'q':
            persist = 0
        
        if persist == 1:
            print "Continue (Y/n)"
            command = raw_input()
            if command == 'n':
                persist = 0