from bitstring import BitArray
import math

class Bitpack:
    def __init__(self):
        self.min = None
        self.max = None
        self.null_bitmap = []
        self.num_elements = 0

    def compress(self, column):
        self.min = min(map(lambda x: x[0], filter(lambda x : x[0] != None, column)))
        self.max = max(map(lambda x: x[0], filter(lambda x : x[0] != None, column)))

        num_bits = math.ceil(math.log(self.max - self.min + 1, 2))
        new_column = [(BitArray(),)]

        for row in column:
            if row[0] != None:
                self.null_bitmap.append(0)
                
                new_column[0][0].append(BitArray(uint = row[0] - self.min, length = num_bits))
            else:
                self.null_bitmap.append(1)
            self.num_elements += 1

        x = len(self.null_bitmap)
        next_multiple_of_8 = 8 * (math.ceil(x / 8) + 1)
        
        while x < next_multiple_of_8:
            self.null_bitmap.append(1)
            x = x+1

        new_column = [(str(new_column[0][0]),)]
        return new_column

    def decompress(self, column):
        pass
