from dictionary_encode import DictionaryEncode
from bitpack import Bitpack

import os
import sqlite3

filename = "questions"
con = sqlite3.connect(os.path.join(os.getcwd(), "data", filename + ".db"))

cur = con.cursor()
column_name_query = "SELECT name FROM PRAGMA_TABLE_INFO('" + filename + "');"
res = cur.execute(column_name_query)

column_names = res.fetchall()
for column_name in column_names:
    query = "SELECT \"" + column_name[0] + "\" FROM " + filename + ";"
    print(query)
    if column_name[0] != "index":
        res = cur.execute(query)
        column = res.fetchall()
print("done")
