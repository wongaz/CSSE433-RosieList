from bigtable_helper import *

user_table = 'Rosie-List-Users'
transaction_table = 'Rosie-List-Transactions'
ride_table = 'Rosie-List-Rides'
review_table = 'Rosie-List-Reviews'
product_table = 'Rosie-List-Products'
tag_table = 'Rosie-List-Tags'


@connect
def remote_reset(conn):
    if has_table(user_table, conn):
        conn.delete_table(user_table)
    conn.create_table(user_table,
                          {'Key': dict(),
                           'Bio': dict(),
                           'Transactions': dict(),
                           }
                          )
    if has_table(transaction_table, conn):
        conn.delete_table(transaction_table)
    conn.create_table(
            transaction_table,
            {'Key': dict(),
             'Users': dict(),
             'Product': dict(),
             }
        )
    if has_table(product_table, conn):
        conn.delete_table(product_table)
    conn.create_table(
            product_table,
            {'Key': dict(),
             'Info': dict(),
             'Tags': dict(),
             }
        )
    if has_table(ride_table, conn):
        conn.delete_table(ride_table)
    conn.create_table(
            ride_table,
            {'Key': dict(),
             'Users': dict(),
             'Info': dict(),
             }
        )
    if has_table(review_table, conn):
        conn.delete_table(review_table)
    conn.create_table(
            review_table,
            {'Key': dict(),
             'Users': dict(),
             'Info': dict(),
             }
        )
    if has_table(tag_table, conn):
        conn.delete_table(tag_table)
    conn.create_table(
            tag_table,
            {'Key': dict(),
             'Info': dict(),
             }
        )


@connect
def create_user(conn, user, f_name, l_name, email):
    table = conn.table(user_table)
    table.put(user, {b'Key:user': user,
                     b'Bio:fName': f_name, b'Bio:lName': l_name, b'Bio:email': email,
                     b'Transactions:t_history': "", b'Transactions:r_history': "",
                     b'Transactions:products': "", b'Transactions:reviews': ""})


@connect
def create_transaction(conn, tid, buyer, seller, pid):
    table = conn.table(transaction_table)
    table.put(tid, {b'Key:TID': tid,
                    b'Users:buyer': buyer, b'Users:seller': seller,
                    b'Product:PID': pid})


@connect
def create_product(conn, pid, name, desc, tags, price):
    table = conn.table(product_table)
    string_tags = convert_array_to_string(tags)
    table.put(pid, {b'Key:PID': pid,
                    b'Info:name': name, b'Info:description': desc, b'Info:price': price,
                    b'Tags:tags': string_tags})


@connect
def create_ride(conn, rid, driver, rider, dest, miles, price):
    table = conn.table(ride_table)
    table.put(rid, {b'Key:RID': rid,
                    b'Users:driver': driver, b'Users:rider': rider,
                    b'Info:destination': dest, b'Info:mileage': miles, b'Info:price': price})


@connect
def create_review(conn, rvid, patron, provider, contents):
    table = conn.table(review_table)
    table.put(rvid, {b'Key:RVID': rvid,
                     b'Users:reviewer': patron, b'Users:reviewed': provider,
                     b'Info:contents': contents})


@connect
def create_tag(conn, tgid, name):
    table = conn.table(tag_table)
    table.put(tgid, {b'Key:TGID': tgid,
                     b'Info:name': name})
