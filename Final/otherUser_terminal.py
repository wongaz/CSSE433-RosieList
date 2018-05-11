from terminal_helper import *
from local_redis import *
from bigtable_helper import *
from bigtable_lib import *
from bigtable_remote import *


@connect
def displayUser(connection, user):
    table = connection.table(userTable)
    row = table.row(user)
    if(not hasRow(user, userTable)):
        print("Username not in database")
        return
    print("Username:", end=' ')
    print((row[b'Key:user']))
    print("Name:", end=' ')
    print((row[b'Bio:lName'] + ", " + row[b'Bio:fName'])) 
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
        job = q.enqueue(edit_bio_name, user, fName, lName)
        while job.result is None:
            continue
        if job.result == 1:
            print("User was not updated")
    if command == '2':
        print("Enter a new email")
        email = input()
        job = q.enqueue(edit_bio_email, user, email)
        while job.result is None:
            continue
        if job.result == 1:
            print("User was not updated")

@connect
def leaveReview(connection, user, loggedInUser):
    print('Enter Review ID')
    rvid = input()
    if(hasRow(rvid, reviewTable)):
        print("Review ID already in database")
        return
    if(rvid == ""):
        print("Must enter a Review ID")
        return
    patron = loggedInUser
    provider = user
    print('Enter contents of review')
    contents = input()
    if(contents == ""):
        print("Must enter contents for review")
        return
    job = q.enqueue(create_review(conn, rvid, patron, provider, contents))
    while job.result is None:
        continue
    addReviewToUser(provider, rvid)

@connect
def registerRide(connection, user, loggedInUser):
    print('Enter Ride ID')
    rid = input()
    if(hasRow(rid, rideTable)):
        print("Ride ID already in database")
        return
    if(rid == ""):
        print("Must enter a Ride ID")
        return
    print('Are you a driver(d) or passenger(p)')
    command = input()
    if(command == 'd'):
        rider = user
        driver = loggedInUser
    else:
        rider = loggedInUser
        driver = user
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
    job = q.enqueue(create_ride(rid, driver, rider, dest, miles, price))
    while job.result is None:
        continue
    addRideToUsers(driver, rider, rid) 
    write_history(loggedInUser, 'r' + rid)
    write_history(user, 'r' + rid)  

@connect
def otherUserTerminal(connection, user, loggedInUser):
    persist = 1
    write_history(loggedInUser, 'u' + user)
    while(persist == 1):
        print("Enter a command:")
        print("1 - Print Bio")
        print("2 - Leave Review")
        print("3 - Register ride")
        print("q - Return to user hub menu")
        command = input()
        if command == '1':
            displayUser(user)

        if command == '2':
            leaveReview(user, loggedInUser)

        if command == '3':
            registerRide(user, loggedInUser)

        if command == 'q':
            persist = 0
        
        if persist == 1:
            print("Continue (Y/n)")
            command = input()
            if command == 'n':
                persist = 0

@connect
def userHubTerminal(connection, user):
    persist = 1
    while(persist == 1):
        print("Enter a command:")
        print("1 - List users")
        print("2 - Visit user page")
        print("q - Return to root menu")
        command = input()

        if command == '1':
            table = connection.table(userTable)
            for key, data in table.scan():
                print("Username: " + key, end=' ')
                print("Name:", end=' ')
                print((data[b'Bio:fName'] + " " + data[b'Bio:lName'])) 
        
        if command == '2':
            table = connection.table(userTable)
            print('Enter Username')
            otherUsername = input()
            if(not hasRow(otherUsername, userTable)):
                print("Username not in database")
            else:
                if(user == otherUsername):
                    print("Already logged in as " + user)
                else:
                    otherUserTerminal(otherUsername, user)

        if command == 'q':
            persist = 0

        if persist == 1:
            print("Continue (Y/n)")
            command = input()
            if command == 'n':
                persist = 0
