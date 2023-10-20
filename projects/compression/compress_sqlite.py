from compression_schemes.dictionary_encode import DictionaryEncode
from compression_schemes.bitpack import Bitpack

import os
import sys

import sqlite3
from decimal import *

def try_cast_to_int(column):
    pass

def try_cast_to_decimal(column):
    pass

def try_dictionary_encode(column):
    dictionary_encode = DictionaryEncode()
    dictionary_encode.compress(column)

def try_bitpack(column):
    pass

def compress(filename):
    filename = "clothes"
    con = sqlite3.connect(os.path.join(os.getcwd(), "data", filename + ".db"))

    
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

def decompress(filename):
    pass

compress("clothes")
