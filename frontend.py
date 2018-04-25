from bigtable_lib import *

# Main


while True:
    print("Enter a command:")
    print("1 - Display Users")
    print("2 - Add User")
    print("3 - Display Transactions")
    print("4 - Add Transaction")
    print("5 - Display Product")
    print("6 - Add Product")
    print("7 - Display Ride")
    print("8 - Add Ride")
    print("9 - Display Review")
    print("10 - Add Review")
    print("11 - Display Tag")
    print("12 - Add Tag")
    print("R - Reset")
    print("C - Clear")
    print("q - Quit")
    command = input()
    func_dict = {'1': display_users,
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
                 'R': reset,
                 'C': clear}

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
