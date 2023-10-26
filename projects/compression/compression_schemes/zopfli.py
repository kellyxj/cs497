import zopfli
import curses.ascii

class Zopfli():
    def __init__(self):
        pass

    def compress(self, column):
        input_string = ""
        delimiter = curses.ascii.ascii(127)
        for row in column:
            input_string += str(row[0])  + chr(delimiter)
        print(len(input_string))
        input_string = input_string[:len(input_string)-1]
        c = zopfli.ZopfliCompressor(zopfli.ZOPFLI_FORMAT_DEFLATE)
        z = c.compress(bytes(input_string, "utf-8")) + c.flush()
        return [(z,)]

    def decompress(self, column):
        pass
