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
        input_string = input_string[:len(input_string)-1]
        c = zopfli.ZopfliCompressor(zopfli.ZOPFLI_FORMAT_DEFLATE)
        z = c.compress(bytes(input_string, "utf-8")) + c.flush()
        return [(z,)]

    def decompress(self, column):
        raw = column[0][0]
        delimiter = curses.ascii.ascii(127)
        d = zopfli.ZopfliDecompressor(zopfli.ZOPFLI_FORMAT_DEFLATE)
        input_string = d.decompress(raw) + d.flush()
        column = input_string.decode("utf-8").split(chr(delimiter))
        column = list(map(lambda x: (x,), column))
        return column
