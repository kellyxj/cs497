from util.get_size import get_obj_size

class DictionaryEncode():
    def __init__(self):
        pass

    def compress(self, column):
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

    def decompress(self, column):
        pass
