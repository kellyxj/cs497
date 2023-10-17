from dictionary_encode import DictionaryEncode
from bitpack import Bitpack

import os
import gc
import sys
import sqlite3

#from https://stackoverflow.com/questions/13530762/how-to-know-bytes-size-of-python-object-like-arrays-and-dictionaries-the-simp
def get_obj_size(obj):
    marked = {id(obj)}
    obj_q = [obj]
    sz = 0

    while obj_q:
        sz += sum(map(sys.getsizeof, obj_q))

        all_refr = ((id(o), o) for o in gc.get_referents(*obj_q))

        new_refr = {o_id: o for o_id, o in all_refr if o_id not in marked and not isinstance(o, type)}

        obj_q = new_refr.values()
        marked.update(new_refr.keys())

    return sz

def try_dictionary_encode(column):
    dictionary = {}
    new_column = []
    count = 0
    for row in column:
        if not row[0] in dictionary:
            dictionary[row[0]] = count
            count += 1
        new_column.append(dictionary[row[0]])
    print("Original size: " + str(get_obj_size(column)))
    print("Dictionary size: " + str(get_obj_size(dictionary)))
    print("New column size: " + str(get_obj_size(new_column)))

def try_bitpack(column):
    pass

filename = "clothes"
con = sqlite3.connect(os.path.join(os.getcwd(), "data", filename + ".db"))

dictionary_encode = DictionaryEncode()
bitpack = Bitpack()

cur = con.cursor()
column_name_query = "SELECT name FROM PRAGMA_TABLE_INFO('" + filename + "');"
res = cur.execute(column_name_query)

column_names = res.fetchall()
for column_name in column_names:
    query = "SELECT \"" + column_name[0] + "\" FROM " + filename + ";"
    print(query) 

    if column_name[0] != "index":
        type_query = "SELECT type FROM PRAGMA_TABLE_INFO('" + filename + "') WHERE name = \"" + column_name[0] + "\";"
        res = cur.execute(type_query)
        column_type =  res.fetchall()[0][0]

        res = cur.execute(query)
        column = res.fetchall()

        if column_type == "TEXT":
           try_dictionary_encode(column)
print("done")
