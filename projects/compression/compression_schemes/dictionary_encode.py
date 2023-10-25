class DictionaryEncode():
    def __init__(self):
        self.dictionary = {}

    def compress(self, column):
        new_column = []
        count = 0
        for row in column:
            if not row[0] in self.dictionary:
                self.dictionary[row[0]] = count
                count += 1
            new_column.append((self.dictionary[row[0]],))
        
        return new_column

    def decompress(self, column):
        pass
