from compression_schemes.dictionary_encode import DictionaryEncode
from compression_schemes.zopfli import Zopfli
from compression_schemes.bitpack import Bitpack
from util.get_size import get_obj_size 
from bitstring import BitArray

import os
import sys

import sqlite3
from decimal import *

import base64

class Runner():
    def __init__(self, filename):
        self.filename = filename
        self.read_con = sqlite3.connect(os.path.join(os.getcwd(), "data", filename + ".db"))
        self.read_cursor = self.read_con.cursor()
        self.write_con = sqlite3.connect(os.path.join(os.getcwd(), "data", filename + "_compressed.db"))
        self.write_cursor = self.write_con.cursor()

        self.dictionary_encode = DictionaryEncode()
        self.bitpack = Bitpack()
        self.zopfli = Zopfli()

        self.cast_succeeded = False
        self.dictionary_encoded = False
        self.bitpacked = False
        self.zipped = False

    def try_cast_to_int(self, column):
        new_column = []
        try:
            for row in column:
                new_row = row if row[0] == None else (int(row[0]),)
                new_column.append(new_row)
            self.cast_succeeded = True
            return new_column
        except(TypeError, ValueError):
            self.cast_succeeded = False
            return column

    def try_cast_to_decimal(self, column):
        pass

    def try_dictionary_encode(self, column):
        new_column = self.dictionary_encode.compress(column)

        original_size = get_obj_size(column)
        dictionary_size = get_obj_size(self.dictionary_encode.dictionary) + get_obj_size(self.dictionary_encode.dictionary.keys())
        new_column_size = get_obj_size(new_column)

        print("Original size: " + str(original_size))
        print("Dictionary size: " + str(dictionary_size))
        print("New column size: " + str(new_column_size))

        if len(self.dictionary_encode.dictionary.keys()) < .5 * len(list(filter(lambda x : x != None, column))):
            self.dictionary_encoded = True
            return new_column
        else:
            self.dictionary_encoded = False
            return column

    def try_bitpack(self, column):
        self.bitpack = Bitpack()
        new_column = self.bitpack.compress(column)

        original_size = get_obj_size(column)
        new_column_size = get_obj_size(new_column)
        bitpacked_size = new_column_size + get_obj_size(self.bitpack.null_bitmap)

        print("Original size: " + str(original_size))
        print("Bitpacked size: " + str(bitpacked_size))

        if bitpacked_size < original_size:
            self.bitpacked = True
            return new_column
        else:
            self.bitpacked = False
            return column

    def try_zopfli(self, column):
        new_column = self.zopfli.compress(column)

        original_size = get_obj_size(column)
        new_column_size = get_obj_size(new_column)

        print("Original size: " + str(original_size))
        print("Zipped size: " + str(new_column_size))

        if new_column_size < original_size:
            self.zipped = True
            return new_column
        else:
            self.zipped = False
            return column

    def compress(self):
        self.create_metadata()
        self.vertical_partition_and_dictionary_encode()
        self.compress_partitioned_table()
        self.write_con.execute("VACUUM")

    def vertical_partition_and_dictionary_encode(self):
 
        column_name_query = "SELECT name FROM PRAGMA_TABLE_INFO('" + self.filename + "');"
        res = self.read_cursor.execute(column_name_query)


        column_names = res.fetchall()
        for column_name in column_names:
            #start by creating a metadata table
            #for each column in the original table, store a bit vector
            #set the bit to 1 if compression scheme is applied

            #for each column, try casting to int
            #TODO: if that fails, try casting to float

            #for each column, try each compression scheme and see if the result is smaller

            query = "SELECT \"" + column_name[0] + "\" FROM " + self.filename + ";"
            print(query)


            if column_name[0] != "index" and column_name[0] != "_corrupt_record":
                type_query = "SELECT type FROM PRAGMA_TABLE_INFO('" + self.filename + "') WHERE name = \"" + column_name[0] + "\";"
                res = self.read_cursor.execute(type_query)
                column_type =  res.fetchall()[0][0]

                res = self.read_cursor.execute(query)
                column = res.fetchall()

                if column_type == "TEXT":
                   column = self.try_cast_to_int(column)

                column_type = "INTEGER" if self.cast_succeeded  else column_type
                table_name = column_name[0].replace(" ", "_")
                create_table_statement = "CREATE TABLE IF NOT EXISTS " + table_name + "(\"" + column_name[0] + "\" " + column_type + ");"
                self.write_cursor.execute(create_table_statement)

                insert_statement = "INSERT INTO " + table_name + "(\"" + column_name[0] + "\")" + " VALUES (?);"

                self.write_cursor.executemany(insert_statement, column)
                self.write_con.commit()

                drop_table_statement = "DROP TABLE " + table_name

                if column_type == "TEXT":
                    column = self.try_dictionary_encode(column)
                    if self.dictionary_encoded:
                        create_table_statement = "CREATE TABLE IF NOT EXISTS " + table_name + "_dictionary_encoded" + "(\"key\" INTEGER);"
                        self.write_cursor.execute(create_table_statement)

                        insert_statement = "INSERT INTO " + table_name + "_dictionary_encoded" + "(\"key\")" + " VALUES (?);"
                        self.write_cursor.executemany(insert_statement, column)

                        create_table_statement = "CREATE TABLE IF NOT EXISTS " + table_name + "_dictionary" + "(\"key\" INTEGER, \"" + column_name[0] + "\");"
                        self.write_cursor.execute(create_table_statement)

                        dictionary = self.dictionary_encode.dictionary
                        for value in dictionary.keys():
                            value_string = value.replace("'","''") if value != None else "NULL"
                            insert_statement = "INSERT INTO " + table_name + "_dictionary" + "(\"key\", \"" + column_name[0] + "\") VALUES (" + str(dictionary[value])  + ", '" + value_string + "');"
                            self.write_cursor.execute(insert_statement)

                        self.write_cursor.execute(drop_table_statement)

                        update_metadata_statement = "UPDATE metadata SET dictionary_encoded = 1 WHERE column_name = \"" + column_name[0] + "\";"
                        self.write_cursor.execute(update_metadata_statement)
                        self.write_con.commit()

                self.dictionary_encode.dictionary = {}
        print("done")

    def compress_partitioned_table(self): 
        column_name_query = "SELECT name FROM PRAGMA_TABLE_INFO('" + self.filename + "');"
        res = self.read_cursor.execute(column_name_query)

        column_names = res.fetchall()
        for column_name in column_names:
            if column_name[0] != "index" and column_name[0] != "_corrupt_record":
                table_name = column_name[0].replace(" ", "_")
                
                #check metadata table to see if the column was dictionary encoded
                metadata_lookup = "SELECT dictionary_encoded FROM metadata WHERE column_name = \"" + column_name[0] + "\";"
                res = self.write_cursor.execute(metadata_lookup)

                is_dictionary_encoded = res.fetchall()
                self.dictionary_encoded = is_dictionary_encoded[0][0] == 1

                if self.dictionary_encoded:
                    table_name = table_name + "_dictionary_encoded"  

                col_name = "key" if self.dictionary_encoded else column_name[0]

                query = "SELECT \"" + col_name + "\" FROM " + table_name + ";"
                print(query)

                type_query = "SELECT type FROM PRAGMA_TABLE_INFO('" + table_name + "') WHERE name = \"" + col_name + "\";"

                res = self.write_cursor.execute(type_query)
                column_type =  res.fetchall()[0][0]
                
                res = self.write_cursor.execute(query)
                column = res.fetchall()

                if column_type == "INTEGER":
                    column = self.try_bitpack(column)
                    if self.bitpacked:
                        update_metadata_statement = "UPDATE metadata SET bitpacked = 1 WHERE column_name = \"" + column_name[0] + "\";"
                        self.write_cursor.execute(update_metadata_statement)

                        update_metadata_statement = "UPDATE metadata SET min = " + str(self.bitpack.min) + " WHERE column_name = \"" + column_name[0] + "\";"
                        self.write_cursor.execute(update_metadata_statement)

                        update_metadata_statement = "UPDATE metadata SET max = " + str(self.bitpack.max) +  " WHERE column_name = \"" + column_name[0] + "\";"
                        self.write_cursor.execute(update_metadata_statement)

                        update_metadata_statement = "UPDATE metadata SET null_bitmap = " + str(base64.b64encode(BitArray(self.bitpack.null_bitmap).bytes)).replace("/", "_")[1:] + " WHERE column_name = \"" + column_name[0] + "\";"
                        self.write_cursor.execute(update_metadata_statement)

                        drop_table_statement = "DROP TABLE " + table_name + ";"
                        self.write_cursor.execute(drop_table_statement)

                        create_table_statement = "CREATE TABLE " + table_name + "(\"" + col_name + "\" TEXT);"
                        self.write_cursor.execute(create_table_statement)

                        insert_statement = "INSERT INTO " + table_name + "(\"" + col_name + "\") VALUES (?);"
                        self.write_cursor.executemany(insert_statement, column)

                        self.write_con.commit()

                column = self.try_zopfli(column)
                if self.zipped:
                    update_metadata_statement = "UPDATE metadata SET zipped = 1 WHERE column_name = \"" + column_name[0] + "\";"
                    self.write_cursor.execute(update_metadata_statement)

                    drop_table_statement = "DROP TABLE " + table_name
                    self.write_cursor.execute(drop_table_statement)

                    write_column = "key" if self.dictionary_encoded else column_name[0]
                    create_table_statement = "CREATE TABLE " + table_name + "(\"" + write_column + "\" TEXT);"
                    self.write_cursor.execute(create_table_statement)

                    insert_statement = "INSERT INTO " + table_name + "(\"" + write_column + "\") VALUES (?);"
                    self.write_cursor.executemany(insert_statement, column)

                    self.write_con.commit()

                metadata_lookup = "SELECT bitpacked FROM metadata WHERE column_name = \"" + column_name[0] + "\";"
                res = self.write_cursor.execute(metadata_lookup)

                is_bitpacked = res.fetchall()
                self.bitpacked = is_bitpacked[0][0] == 1

                if self.bitpacked:
                    metadata_lookup = "SELECT null_bitmap FROM metadata WHERE column_name = \"" + column_name[0] + "\";"
                    res = self.write_cursor.execute(metadata_lookup)
                    bitpack_metadata = res.fetchall()
                    update_statement = "UPDATE metadata SET null_bitmap = " + str(base64.b64encode(self.try_zopfli(bitpack_metadata)[0][0])).replace("/", "_")[1:] + " WHERE column_name = \"" + column_name[0] + "\";"

                    self.write_cursor.execute(update_statement)
                    self.write_con.commit()


    def decompress(self):
        #for each column, check the bit in the metadata table to see if compression scheme was applied
        #if it was applied, read from the correct auxiliary table to decompress
        column_name_query = "SELECT name FROM PRAGMA_TABLE_INFO('" + self.filename + "');"
        res = self.read_cursor.execute(column_name_query)

        column_names = res.fetchall()
        for column_name in column_names:
            if column_name[0] != "index" and column_name[0] != "_corrupt_record":
                table_name = column_name[0].replace(" ", "_")
                
                #check metadata table to see if the column was dictionary encoded
                metadata_lookup = "SELECT dictionary_encoded FROM metadata WHERE column_name = \"" + column_name[0] + "\";"
                res = self.write_cursor.execute(metadata_lookup)

                is_dictionary_encoded = res.fetchall()
                self.dictionary_encoded = is_dictionary_encoded[0][0] == 1

                if self.dictionary_encoded:
                    table_name = table_name + "_dictionary_encoded"

                query = "SELECT \"" + column_name[0] + "\" FROM " + table_name + ";"

                res = self.write_cursor.execute(query)
                column = res.fetchall()

                metadata_lookup = "SELECT zipped FROM metadata WHERE column_name = \"" + column_name[0] + "\";"
                res = self.write_cursor.execute(metadata_lookup)

                is_zipped = res.fetchall()
                self.zipped = is_zipped[0][0] == 1

                if self.zipped:
                    column = self.zopfli.decompress(column)

               
    def create_metadata(self):
        column_name_query = "SELECT name FROM PRAGMA_TABLE_INFO('" + self.filename + "');"
        res = self.read_cursor.execute(column_name_query)

        column_names = res.fetchall()

        create_metadata_query = "CREATE TABLE IF NOT EXISTS metadata(column_name TEXT, dictionary_encoded INTEGER, bitpacked INTEGER, zipped INTEGER, min INTEGER, max INTEGER, null_bitmap TEXT, num_elements INTEGER);"
        self.write_cursor.execute(create_metadata_query)

        for column_name in column_names:
            insert_query = "INSERT INTO metadata(column_name, dictionary_encoded, bitpacked, zipped, min, max, null_bitmap, num_elements) VALUES (\"" + column_name[0] + "\",0,0,0, NULL, NULL, NULL, NULL);"
            self.write_cursor.execute(insert_query)

runner = Runner("clothes")
runner.compress()
#runner.decompress()

runner = Runner("news")
runner.compress()

runner = Runner("questions")
runner.compress()
