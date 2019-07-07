#!/usr/bin/env python
import sys
import os
import gzip


def getqualitysystem(fqfile, line_num=100):
    if os.path.splitext(fqfile)[1] == '.gz':
        inputFile = gzip.open(fqfile, 'rt')
    else:
        inputFile = open(fqfile, 'r')

    qs_set = set()
    n = 0
    for line in inputFile:
        if n > line_num:
            break
        n += 1
        if n % 4 == 0:
            qs_set.update(line[:-1])
    min = 'h'
    max = '!'
    for c in qs_set:
        min = min > c and c or min
        max = max < c and c or max
    if max > 'J' and min > ':':
        return 1
    elif max <= 'K' and min <= ':':
        return 0
    elif max <= 'K' and min > ':':
        return -1
    else:
        print("Cann't check quilitySystem, please set it in user.cfg: init.qualitysystem!")
        exit(1)


def main():
    if len(sys.argv) < 2:
        print("Usage:\n\tpython {} <fq.gz>".format(sys.argv[0]))
        exit(-1)
    r = getqualitysystem(sys.argv[1])
    if r == 1:
        print("64")
    elif r == 0:
        print("33")
    else:
        print("-1")
    return r


if __name__ == "__main__":
    sys.exit(main())
