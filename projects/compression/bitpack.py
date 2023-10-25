import sqlite3
import os

class Bitpack():
    def __init__(self):
        pass

    def compress(self, column):
        pass

    def decompress(self, column):
        pass

filename = "clothes"
con = sqlite3.connect(os.path.join(os.getcwd(), "data", filename + ".db"))
cursor = con.cursor()

# Retrieve data from the 'hips' column
cursor.execute('SELECT hips FROM clothes')
values_to_pack = cursor.fetchall()

bit_width = 32

# Map NULL values to a sentinel value -1
cursor.execute('UPDATE clothes SET hips = ? WHERE hips IS NULL', (-1,))

# Retrieve and bit-pack non-NULL values
cursor.execute('SELECT hips FROM clothes WHERE hips IS NOT NULL')
values_to_pack = cursor.fetchall()

cursor.execute('ALTER TABLE clothes ADD COLUMN bit_packed_hips INT')

# Bit-pack and update the non-NULL values
for value in values_to_pack:
    original_value = int(float(value[0]))
    # print(type(original_value))
    packed_value = original_value & ((1 << bit_width) - 1)
    
    cursor.execute('UPDATE clothes SET bit_packed_hips = ? WHERE hips = ?', (packed_value, original_value))

con.commit()

# Close the database connection
con.close()