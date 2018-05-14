from personal_information import *
from product_information import *
from otherUser_terminal import *
from terminal_helper import *

userTable = 'Rosie-List-Users'
transactionTable = 'Rosie-List-Transactions'
rideTable = 'Rosie-List-Rides'
reviewTable = 'Rosie-List-Reviews'
productTable = 'Rosie-List-Products'
tagTable = 'Rosie-List-Tags'

def userTerminal(username):
    persist = 1
    while(persist == 1):
        print("Enter a command:")
        print("1 - Personal Information")
        print("2 - Products")
        print("3 - Other Users")
        print("q - logout")
        command = input()

        if command == '1':
            personalTerminal(username)
            
        if command == '2':
            productTerminal(username)

        if command == '3':
            userHubTerminal(username)

        if command == 'q':
            persist = 0
        
        
        if persist == 1:
            print("Continue (Y/n)")
            command = input()
            if command == 'n':
                persist = 0

def login():
    neo4j_lib.get_recom_sys()
    inputUsername = 0
    while inputUsername == 0:
        print('Enter Username (q to return to main menu)')
        user = input()
        if user == 'q':
            return
        if(not hasRow(user, userTable)):
            print("Username not in database")
        else:
            userTerminal(user)
    return

def createAccount():
    addUser()
    
#Main
persist = 1
while(persist == 1):
    print("Enter a command:")
    print("1 - Login")
    print("2 - Create user")
    print("q - Quit")
    command = input()
    if command == '1':
        login()
    
    if command == '2':
        createAccount()

    if command == 'q':
        persist = 0
    
    if persist == 1:
        print("Continue (Y/n)")
        command = user = input()
        if command == 'n':
            persist = 0