from  bigtable_helper import *
from  bigtable_lib import *
from  local_redis import *
from  redis_lib import *
from  terminal_helper import *

userTable = 'Rosie-List-Users'
transactionTable = 'Rosie-List-Transactions'
rideTable = 'Rosie-List-Rides'
reviewTable = 'Rosie-List-Reviews'
productTable = 'Rosie-List-Products'
tagTable = 'Rosie-List-Tags'


@connect
def displayUser(connection,user):
    table = connection.table(userTable)
    row = table.row(user.encode('utf-8'))
    if(not hasRow(user, userTable)):
        print("Username not in database")
        return
    print("Username:", end=' ')
    print((row[b'Key:user']))
    print("Name:", end=' ')
    print((row[b'Bio:lName'] + ", ".encode("utf-8") + row[b'Bio:fName']))
    print("Email:", end=' ')
    print((row[b'Bio:email']))
    tHistory = convertStringToArray(row[b'Transactions:tHistory'])
    rHistory = convertStringToArray(row[b'Transactions:rHistory'])
    products = convertStringToArray(row[b'Transactions:products'])
    reviews = convertStringToArray(row[b'Transactions:reviews'])
    printTransactionArray(tHistory, user)
    printRideArray(rHistory, user)
    printProductArray(products)
    printReviewArray(reviews)

@connect
def editBio(connection, user):
    table = connection.table(userTable)
    print("Select a field to edit:")
    print("1 - Name")
    print("2 - Email")
    command = input()
    if command == '1':
        print("Enter a new first name")
        fName = input()
        print("Enter a new last name")
        lName = input()

        q.enqueue(edit_bio_name,user,fName,lName)

    if command == '2':
        print("Enter a new email")
        email = input()
        q.enqueue(edit_bio_email, user, email)

@connect
def listTransactions(connection,user):
    uTable = connection.table(userTable)
    uRow = uTable.row(user.encode('utf-8'))
    tHistory = convertStringToArray(uRow[b'Transactions:tHistory'])
    for tid in tHistory:
        table = connection.table(transactionTable)
        row = table.row(tid.encode('utf-8'))
        print("")
        print("Transaction ID:", end=' ')
        print((row[b'Key:TID']))
        print("Buyer Username:", end=' ')
        print((row[b'Users:buyer']))
        print("Seller Username:", end=' ')
        print((row[b'Users:seller'])) 
        print("Product ID:", end=' ')
        print((row[b'Product:PID']))

@connect
def deleteTransactionFromUser(connection, user):
    table = connection.table(transactionTable)
    uTable = connection.table(userTable)
    print("Enter a tid")
    tid = input()
    if(not hasRow(tid, transactionTable)):
        print("Rid eID not in database")
        return
    row = uTable.row(user.encode('utf-8'))
    userTransactions = convertStringToArray(row[b'Transactions:tHistory'])
    if not tid in userTransactions:
        print("Transaction does not involve this user, cannot be deleted")
        return
    tRow = table.row(tid.encode('utf-8'))
    buyer = tRow[b'Users:buyer']
    seller = tRow[b'Users:seller']
    q.enqueue(remove_transaction_from_user, buyer, seller, tid)

@connect
def listRides(connection,user):
    uTable = connection.table(userTable)
    uRow = uTable.row(user.encode('utf-8'))
    rHistory = convertStringToArray(uRow[b'Transactions:rHistory'])
    table = connection.table(rideTable)
    for rid in rHistory:
        print("")
        row = table.row(rid.encode('utf-8'))
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
def deleteRideFromUser(connection,user):
    table = connection.table(rideTable)
    uTable = connection.table(userTable)
    print("Enter a rid")
    rid = input()
    if(not hasRow(rid, rideTable)):
        print("Ride ID not in database")
        return
    row = uTable.row(user.encode('utf-8'))
    userRides = convertStringToArray(row[b'Transactions:rHistory'])
    if not rid in userRides:
        print("Ride does not involve this user, cannot be deleted")
        return
    rRow = table.row(rid.encode('utf-8'))
    driver = rRow['Users:driver']
    rider = rRow['Users:rider']
    job = q.enqueue(remove_rider_from_user, rider, driver, rid)


@connect
def listReviews(connection,user):
    uTable = connection.table(userTable)
    uRow = uTable.row(user.encode('utf-8'))
    reviews = convertStringToArray(uRow[b'Transactions:reviews'])
    table = connection.table(reviewTable)
    for rvid in reviews:
        row = table.row(rvid.encode('utf-8'))
        print("")
        print("Review ID:", end=' ')
        print((row[b'Key:RVID']))
        print("Reviewer Username:", end=' ')
        print((row[b'Users:reviewer']))
        print("Reviewed Username:", end=' ')
        print((row[b'Users:reviewed'])) 
        print("Review Contents:", end=' ')
        print((row[b'Info:contents']))

def personalTerminal(user):
    persist = 1
    while(persist == 1):
        print("Enter a command:")
        print("1 - Print Bio")
        print("2 - Edit Bio")
        print("3 - List Transactions")
        print("4 - Delete Transactions")
        print("5 - List Rides")
        print("6 - Delete Rides")
        print("7 - List reviews")
        print("8 - Print history")
        print("q - Return to root menu")
        command = input()
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
                print(data)

        if command == 'q':
            persist = 0
        
        if persist == 1:
            print("Continue (Y/n)")
            command = input()
            if command == 'n':
                persist = 0