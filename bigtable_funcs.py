import os
from google.cloud import bigtable
from google.cloud import happybase
def bigtable_test1():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "key.json"
    project_id= "csse433-adb-201117"
    instance_id ="project433"
    table_name = 'test'

    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    connection = happybase.Connection(instance=instance)

    print('Creating the {} table.'.format(table_name))
    column_family_name = 'test_cf'
    connection.create_table(
        table_name,
        {
            column_family_name: dict()  # Use default options.
        })

    print('Writing some greetings to the table.')
    table = connection.table(table_name)
    column_name = '{fam}:test_c'.format(fam=column_family_name)
    row = [
        'bigtable_info'
    ]

    for i, value in enumerate(row):
        row_key = 'test_c{}'.format(i)
        table.put(row_key, {column_name: value})
    connection.close()

def bigtable_test2():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "key.json"
    project_id = "csse433-adb-201117"
    instance_id = "project433"
    table_name = 'test'

    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    connection = happybase.Connection(instance=instance)
    table = connection.table(table_name)
    column_family_name = 'test_cf'
    column_name = '{fam}:test_c'.format(fam=column_family_name)
    print('Getting a value by row key.')
    key = 'test_c0'.encode('utf-8')
    row = table.row(key)
    print('\t{}: {}'.format(key, row[column_name.encode('utf-8')]))
    connection.close()

def bigtable_test3():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "key.json"
    project_id = "csse433-adb-201117"
    instance_id = "project433"
    table_name = 'test'

    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    connection = happybase.Connection(instance=instance)
    print('Deleting the {} table.'.format(table_name))
    connection.delete_table(table_name)

    connection.close()




