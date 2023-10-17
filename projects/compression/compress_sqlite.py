from dictionary_encode import DictionaryEncode
from bitpack import Bitpack

import os
import sqlite3

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

        #if column_type == TEXT:
        #   try_bitpack(column)
print("done")

def try_dictionary_encode(column):
    pass

def try_bitpack(column):
    pass
