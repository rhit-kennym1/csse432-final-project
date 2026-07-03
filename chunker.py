import hashlib
import os

CHUNK_SIZE = 4096


def chunk_file(filepath):
    chunks = []
    with open(filepath, 'rb') as f:
        index = 0
        while True:
            data = f.read(CHUNK_SIZE)
            if not data:
                break
            checksum = hashlib.md5(data).hexdigest()
            chunks.append((index, checksum, data))
            index += 1
    return chunks


def file_checksums(filepath):
    sums = []
    try:
        with open(filepath, 'rb') as f:
            while True:
                data = f.read(CHUNK_SIZE)
                if not data:
                    break
                sums.append(hashlib.md5(data).hexdigest())
    except FileNotFoundError:
        pass
    return sums
