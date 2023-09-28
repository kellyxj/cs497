import amazon.ion.simpleion as ion
import argparse
import bson
import bz2
import cbor2
import gzip
import json
import lz4.frame
import lzma
import msgpack
import os
import orjson
import pandas as pd
import pyarrow.json as pa
import pyarrow.parquet as pq
import rapidjson
import simplejson
import ujson
import time
import zstandard as zstd

ALGORITHM = ['block', 'parquet']
BINARY =    ['bson', 'cbor2', 'ion', 'minify', 'msgpack']
ENCODING =  ['none', 'brotli', 'bz2', 'gzip', 'lz4', 'lzma', 'zstd']
PARSER =    ['json', 'orjson', 'rapidjson', 'simplejson', 'ujson']

bson.dumps = bson.encode
bson.loads = bson.decode

class minify:
    def dumps(x):
        return json.dumps(x, separators=(',', ':')).encode()

class none:
    def open(outfile, mode, level):
        return open(outfile, mode)

lz4.open = lambda outfile, mode, level: lz4.frame.open(outfile, mode, \
                                                       compression_level=level)

lzma_open = lzma.open
lzma.open = lambda outfile, mode, level: lzma_open(outfile, mode, preset=level)

zstd_open = zstd.open
zstd.open = lambda outfile, mode, level: zstd_open(outfile, mode, \
                                                   zstd.ZstdCompressor(level))

def compress_block(args):
    ops = []
    if args.binary != 'none':
        ops.append(globals()[args.parser].loads)
        ops.append(globals()[args.binary].dumps)
    outfile = globals()[args.encoding].open(args.outfile, 'wb', args.level)

    with open(args.infile, 'rb') as infile:
        for line in infile:
            out = line[:-1]
            for op in ops:
                out = op(out)
            outfile.write(out)

def compress_parquet(args):
    compression_level = None if args.encoding == 'none' else args.level
    table = pa.read_json(args.infile)
    pq.write_table(table, args.outfile, compression=args.encoding,
                   write_statistics=False, compression_level=compression_level,
                   store_schema=False)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('algorithm', choices=ALGORITHM)
    parser.add_argument('infile')
    parser.add_argument('outfile')
    parser.add_argument('-b', '--binary', default='none', choices=BINARY)
    parser.add_argument('-e', '--encoding', default='none', choices=ENCODING)
    parser.add_argument('-l', '--level', default=9, type=int)
    parser.add_argument('-p', '--parser', default='json', choices=PARSER)
    args = parser.parse_args()

    run = globals()[f'compress_{args.algorithm}']
    start = time.time()
    run(args)
    stop = time.time()

    in_size = os.path.getsize(args.infile)
    out_size = os.path.getsize(args.outfile)

    print(f'in bytes:  {in_size}')
    print(f'out bytes: {out_size}')
    print(f'ratio (%): {(out_size / in_size):.2f}')
    print(f'time (s):  {(stop - start):.2f}')

if __name__ == '__main__':
    main()
