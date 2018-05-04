from terminal_helper import *
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

def leaveReview(user, loggedInUser):
    print 'Enter Review ID'
    rvid = raw_input()
    if(hasRow(rvid, reviewTable)):
        print "Review ID already in database"
        return
    if(rvid == ""):
        print "Must enter a Review ID"
        return
    patron = loggedInUser
    provider = user
    print 'Enter contents of review'
    contents = raw_input()
    if(contents == ""):
        print "Must enter contents for review"
        return
    createReview(rvid, patron, provider, contents)
    addReviewToUser(provider, rvid)

def registerRide(user, loggedInUser):
    print 'Enter Ride ID'
    rid = raw_input()
    if(hasRow(rid, rideTable)):
        print "Ride ID already in database"
        return
    if(rid == ""):
        print "Must enter a Ride ID"
        return
    print 'Are you a driver(d) or passenger(p)'
    command = raw_input()
    if(command == 'd'):
        rider = user
        driver = loggedInUser
    else:
        rider = loggedInUser
        driver = user
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
    addRideToUsers(driver, rider, rid)   

def otherUserTerminal(user, loggedInUser):
    persist = 1
    while(persist == 1):
        print "Enter a command:"
        print "1 - Print Bio"
        print "2 - Leave Review"
        print "3 - Register ride"
        print "q - Return to user hub menu"
        command = raw_input()
        if command == '1':
            displayUser(user)

        if command == '2':
            leaveReview(user, loggedInUser)

        if command == '3':
            registerRide(user, loggedInUser)

        if command == 'q':
            persist = 0
        
        if persist == 1:
            print "Continue (Y/n)"
            command = raw_input()
            if command == 'n':
                persist = 0

def userHubTerminal(user):
    persist = 1
    while(persist == 1):
        print "Enter a command:"
        print "1 - List users"
        print "2 - Visit user page"
        print "q - Return to root menu"
        command = raw_input()

        if command == '1':
            table = connection.table(userTable)
            for key, data in table.scan():
                print "Username: " + key,
                print "Name:",
                print(data[b'Bio:fName'] + " " + data[b'Bio:lName']) 
        
        if command == '2':
            table = connection.table(userTable)
            print 'Enter Username'
            otherUsername = raw_input()
            if(not hasRow(otherUsername, userTable)):
                print "Username not in database"
            else:
                if(user == otherUsername):
                    print "Already logged in as " + user
                else:
                    otherUserTerminal(otherUsername, user)

        if command == 'q':
            persist = 0

        if persist == 1:
            print "Continue (Y/n)"
            command = raw_input()
            if command == 'n':
                persist = 0
