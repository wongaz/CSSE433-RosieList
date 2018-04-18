def displayProduct():
    print "Query goes here"

#Main
persist = 1

while(persist == 1):
    print "Enter a command:"
    print "1 - Display Products"
    print "q - Quit"
    command = raw_input()

    if command == '1':
        displayProduct()

        

    if command == 'q':
        persist = 0
    
    
    if persist == 1:
        print "Continue (Y/n)"
        command = user = raw_input()
        if command == 'n':
            persist = 0


