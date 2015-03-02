#!/usr/bin/env python
# coding=utf8

import sys

def remove_watermark(source, destination=None):
    if not destination:
        destination = source.split(".")[0] + "_clean.pdf"
    f = open(source, "rb")
    book_clean = open(destination,"wb")
    book_content = f.read()
    book_lines = book_content.split('\n')
    line_count = len(book_lines)
    ring_size = 9
    ring = []
    i = 0
    while i < line_count:
        if '(www.it-ebooks.info)Tj' in book_lines[i]:
            i += 1
            continue
        if '/URI (http://www.it-ebooks.info/)' in book_lines[i]:
            ring = []
            i += 4
        else:
            if len(ring) == ring_size:
                book_clean.write(ring[0] + '\n')
                ring.pop(0)
            ring.append(book_lines[i])
            i += 1
    for line in ring:
        book_clean.write(line + '\n')


if __name__ == '__main__':
    """A script that removes watermark footer from e-books downloaded
    from www.it-ebook.info
    Usage ./watermark.py source.pdf [destination.pdf]
    """
    remove_watermark(*sys.argv[1:])