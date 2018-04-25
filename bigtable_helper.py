import os
from google.cloud import bigtable
from google.cloud import happybase


def has_table(name_of_table, conn):
    table_list = conn.tables()
    return name_of_table in table_list


def convert_string_to_array(inputString):
    if inputString == "":
        return []
    result = inputString.split("|")
    return result


def has_row(name_of_row, name_of_table, conn):
    table = conn.table(name_of_table)
    row = table.row(name_of_row)
    return any(row)


def print_array(attribute, entities):
    if len(entities) == 0:
        print(attribute + ": None")
    else:
        print(attribute + ":", end=' ')
        for data in entities:
            print(data + ",", end=' ')
        print("")


def convert_array_to_string(array):
    result = ""
    for data in array:
        result = result + "|" + data
    return result[1:]


def connect(func):
    def function_wrapper(*args, **kwargs):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "key.json"
        project_id = "csse433-adb-201117"
        instance_id = "project433"
        client = bigtable.Client(project=project_id, admin=True)
        instance = client.instance(instance_id)
        connection = happybase.Connection(instance=instance)

        func(connection, *args, **kwargs)

        connection.close()
        return 0

    return function_wrapper
