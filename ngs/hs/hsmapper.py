#!/usr/bin/env python

""" Streaming mapper """

# Standard system input/output
import sys

# Define a separator
SEP = ":"
TAB = "\t"

# Cycle current streaming data
for line in sys.stdin:

    # Clean input
    line = line.strip()

    # Skip SAM/BAM headers
    if line[0] == "@":
        continue

    # Use data
    pieces = line.split(TAB)
    mychr = pieces[2]
    mystart = int(pieces[3])
    myseq = pieces[9]

    # For each element with sequencing coverage
    for i in range(mystart, mystart + len(myseq)):
        results = [mychr+SEP+i.__str__(), "1"]
        print TAB.join(results)
