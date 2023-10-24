from compression_schemes.dictionary_encode import DictionaryEncode
from compression_schemes.bitpack import Bitpack
from util.get_size import get_obj_size 

import os
import sys

import sqlite3
from decimal import *

class Runner():
    def __init__(self, filename):
        self.filename = filename
        self.read_con = sqlite3.connect(os.path.join(os.getcwd(), "data", filename + ".db"))
        self.read_cursor = self.read_con.cursor()
        self.write_con = sqlite3.connect(os.path.join(os.getcwd(), "data", filename + "_compressed.db"))
        self.write_cursor = self.write_con.cursor()

    def try_cast_to_int(self, column):
        new_column = []
        try:
            for row in column:
                new_row = (int(row[0]))
                new_column.append(new_row)
            print("cast succeeded")
            return new_column
        except(TypeError, ValueError):
            print("cast failed")
            return column

    def try_cast_to_decimal(self, column):
        pass

    def try_dictionary_encode(self, column):
        dictionary_encode = DictionaryEncode()
        new_column = dictionary_encode.compress(column)

        print("Original size: " + str(get_obj_size(column)))
        print("Dictionary size: " + str(get_obj_size(dictionary_encode.dictionary)))
        print("New column size: " + str(get_obj_size(new_column)))

    def try_bitpack(self, column):
        bitpack = Bitpack()
        new_column = bitpack.compress(column)

        print("Original size: " + str(get_obj_size(column)))
        print("Bitpacking metadata size: ")
        print("New column size: " + str(get_obj_size(new_column)))

    def compress(self):
 
        column_name_query = "SELECT name FROM PRAGMA_TABLE_INFO('" + self.filename + "');"
        res = self.read_cursor.execute(column_name_query)


        column_names = res.fetchall()
        for column_name in column_names:
            #start by creating a metadata table
            #for each column in the original table, store a bit vector
            #set the bit to 1 if compression scheme is applied


            #for each column, try casting to int
            #if that fails, try casting to float

            #for each column, try each compression scheme and see if the result is smaller

            query = "SELECT \"" + column_name[0] + "\" FROM " + self.filename + ";"
            print(query) 

            if column_name[0] != "index":
                type_query = "SELECT type FROM PRAGMA_TABLE_INFO('" + self.filename + "') WHERE name = \"" + column_name[0] + "\";"
                res = self.read_cursor.execute(type_query)
                column_type =  res.fetchall()[0][0]

                res = self.read_cursor.execute(query)
                column = res.fetchall()

                if column_type == "TEXT":
                   self.try_cast_to_int(column)
        print("done")

    def decompress(self):
        #for each column, check the bit in the metadata table to see if compression scheme was applied
        #if it was applied, read from the correct auxiliary table to decompress
        pass

    def create_metadata(self):

        pass

runner = Runner("clothes")
runner.compress()
