#!/usr/bin/env python
import sys


if len(sys.argv) < 3:
    print "python {} <input_file> <output_file>".format(sys.argv[0])
    exit(0)

input = sys.argv[1]
output = sys.argv[2]

with open(input, 'r') as f, open(output, 'w') as wf:
    for n, line in enumerate(f):
        if n < 2:
            continue

        arr = line.split(',')
        Compound = arr[0]
        fields = arr[23:35]
        ratio_list = [Compound]

        if Compound == 'Compound':
            ratio_list.extend(fields)
        else:
            for index, value in enumerate(fields):
                if float(fields[0]) == 0:
                    ratio_list.append('NA')
                else:
                    ratio_list.append(str(float(value)/float(fields[0])))

        wf.write("\t".join(ratio_list))
        wf.write("\n")
