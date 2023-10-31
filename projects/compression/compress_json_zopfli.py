
import os
import zopfli
import zipfile

def compress(filename):
    with zopfli.ZipFile(os.path.join(os.getcwd(), "data", filename + ".zip"), "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(os.path.join(os.getcwd(), "data", filename + ".json"))

compress("clothes")

compress("news")

compress("questions")
