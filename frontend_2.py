from bigtable_lib import *
from bigtable_helper import *
from redis_lib import *
from neo4j_lib import get_recom

userTable = 'Rosie-List-Users'
transactionTable = 'Rosie-List-Transactions'
rideTable = 'Rosie-List-Rides'
reviewTable = 'Rosie-List-Reviews'
productTable = 'Rosie-List-Products'
tagTable = 'Rosie-List-Tags'

@connect
def login(conn):
    inputUsername = None
    while inputUsername is None:
        print('Enter Username (q to return to main menu)')
        user = input()
        if user == 'q':
            return
        if not has_row(user, userTable):
            print("Username not in database")
        else:
            userTerminal(user)
    return


def userTerminal(username):
    persist = 1
    while (persist == 1):
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

# Main
while True:
    print("Enter a command:")
    print("1 - Login")
    print("2 - Create user")
    print("q - Quit")
    print("R - Reset")
    print("RR - Reset Redis")
    print("D - Delete")
    print("q - Quit")
    command = input()
    func_dict = {'1': login,
                 '2': add_user,
                 '3': display_transaction,
                 '4': add_transaction,
                 '5': display_product,
                 '6': add_product,
                 '7': display_ride,
                 '8': add_ride,
                 '9': display_review,
                 '10': add_review,
                 '11': display_tag,
                 '12': add_tag,
                 '13': get_trans_from_cache,
                 '14': get_recom,
                 'R': reset,
                 'D': clear,
                 'RR': cache_flushall}

    if command == 'q':
        break
    else:
        func = func_dict[command]
        if func is None:
            continue
        func()

    print("Continue (Y/n)")
    command = input()
    if command == 'n':
        break
