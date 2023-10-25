import zopfli

class Zopfli():
    def __init__(self):
        pass

    def compress(self, column):
        input_string = ""
        for row in column:
            input_string += str(row[0])
        c = zopfli.ZopfliCompressor(zopfli.ZOPFLI_FORMAT_DEFLATE)
        z = c.compress(bytes(input_string, "utf-8")) + c.flush()
        return (z,)

    def decompress(self, column):
        pass
