from terminal_helper import *

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
    print "13 - Tag Product"
    print "14 - Delete User"
    print "15 - Delete Transaction"
    print "16 - Delete Ride"
    print "17 - Delete Product"
    print "18 - Delete Tag"
    #TODO Delete Things
    #TODO Update Things
    #TODO Create tag on product initilizaion
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

    if command == '13':
        tagProduct()

    if command == '14':
        deleteUser()

    if command == '15':
        deleteTransaction()

    if command == '16':
        deleteRide()

    if command == '17':
        deleteProduct()

    if command == '18':
        deleteTag()

    if command == 'R':
        resetDatabase()
        
        

    if command == 'q':
        persist = 0
    
    
    if persist == 1:
        print "Continue (Y/n)"
        command = user = raw_input()
        if command == 'n':
            persist = 0