import struct
import sqlite3
import sys


class Bitpacker:
    def __init__(self, filename):
        self.filename = filename
        self.read_conn = sqlite3.connect(filename, uri=True)
        self.read_cursor = self.read_conn.cursor()

    def __del__(self):
        self.read_conn.close()

    def get_obj_size(self, obj):
        return sys.getsizeof(obj)

    def bitpack(self, int_list):
        # Determine the number of bits required to represent the largest integer in the list
        max_int = max(int_list)
        num_bits = max(1, (max_int).bit_length())

        # Pack the integers into a bit-packed byte array
        packed_bits = bytearray()
        current_byte = 0
        bits_remaining = 8
        for i in int_list:
            current_byte = (current_byte << num_bits) | i
            bits_remaining -= num_bits
            if bits_remaining < 0:
                packed_bits.append(current_byte)
                current_byte = i >> abs(bits_remaining)
                bits_remaining = 8 - abs(bits_remaining)
        if bits_remaining != 8:
            current_byte <<= bits_remaining
            packed_bits.append(current_byte)

        return packed_bits

    def compress(self):
        # Create a new database file for the compressed data
        write_filename = self.filename.split(".")[0] + "_compressed.db"
        self.write_conn = sqlite3.connect(write_filename, uri=True)
        self.write_cursor = self.write_conn.cursor()

        # Copy the schema of the original database to the new database
        schema_query = "SELECT sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
        res = self.read_cursor.execute(schema_query)
        schema = res.fetchall()
        for table in schema:
            self.write_cursor.execute(table[0])

        # Iterate over each column in each table and compress the data
        column_name_query = "SELECT name FROM PRAGMA_TABLE_INFO('" + self.filename + "');"
        res = self.read_cursor.execute(column_name_query)
        column_names = res.fetchall()
        for column_name in column_names:
            # Try casting each value in the column to an integer
            query = "SELECT \"" + column_name[0] + "\" FROM " + self.filename + ";"
            res = self.read_cursor.execute(query)
            column = res.fetchall()
            int_column = []
            for value in column:
                try:
                    int_value = int(value[0])
                    int_column.append(int_value)
                except ValueError:
                    pass

            # Determine the number of bits required to represent the range of values in the column
            if len(int_column) > 0:
                max_int = max(int_column)
                min_int = min(int_column)
                range_int = max_int - min_int
                num_bits = max(1, (range_int).bit_length())
            else:
                num_bits = 1

            # Try each compression scheme and see if the result is smaller
            compressed_data = None
            for scheme in ["bitpack"]:
                if scheme == "bitpack":
                    packed_data = self.bitpack([i - min_int for i in int_column])
                    if len(packed_data) < len(column) * 4:
                        compressed_data = packed_data
                        compression_scheme = scheme
                        break

            # If a compression scheme was found, store the compressed data in the new database
            if compressed_data is not None:
                # Create a metadata table for the compressed column
                metadata_query = "CREATE TABLE IF NOT EXISTS " + column_name[0] + "_metadata (compression_scheme TEXT, num_bits INTEGER, min_value INTEGER, packed_data BLOB);"
                self.write_cursor.execute(metadata_query)

                # Insert the compression scheme, number of bits, minimum value, and packed data into the metadata table
                insert_query = "INSERT INTO " + column_name[0] + "_metadata (compression_scheme, num_bits, min_value, packed_data) VALUES (?, ?, ?, ?);"
                self.write_cursor.execute(insert_query, (compression_scheme, num_bits, min_int, compressed_data))

                # Create a new table for the compressed column
                create_query = "CREATE TABLE IF NOT EXISTS " + column_name[0] + " (id INTEGER PRIMARY KEY, compressed_data_id INTEGER);"
                self.write_cursor.execute(create_query)

                # Insert the compressed data IDs into the new table
                select_query = "SELECT rowid FROM " + column_name[0] + "_metadata;"
                res = self.write_cursor.execute(select_query)
                compressed_data_ids = res.fetchall()
                for i, row in enumerate(column):
                    if row[0] is None:
                        self.write_cursor.execute("INSERT INTO " + column_name[0] + " (id, compressed_data_id) VALUES (?, ?);", (i+1, None))
                    else:
                        compressed_data_id = compressed_data_ids[int_column.index(row[0])][0]
                        self.write_cursor.execute("INSERT INTO " + column_name[0] + " (id, compressed_data_id) VALUES (?, ?);", (i+1, compressed_data_id))

            # If no compression scheme was found, store the original column data in the new database
            else:
                # Create a new table for the uncompressed column
                create_query = "CREATE TABLE IF NOT EXISTS " + column_name[0] + " (id INTEGER PRIMARY KEY, value INTEGER);"
                self.write_cursor.execute(create_query)

                # Insert the original column data into the new table
                for i, row in enumerate(column):
                    if row[0] is None:
                        self.write_cursor.execute("INSERT INTO " + column_name[0] + " (id, value) VALUES (?, ?);", (i+1, None))
                    else:
                        self.write_cursor.execute("INSERT INTO " + column_name[0] + " (id, value) VALUES (?, ?);", (i+1, row[0]))

        # Commit the changes and close the new database connection
        self.write_conn.commit()
        self.write_conn.close()