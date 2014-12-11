#!/usr/bin/env python

""" Streaming mapper """

# Defines
SEP = ":"
TAB = "\t"

# ########################
# # In case of BZIP2 compressed file
# # (generated with $ bzip2 -zvc data/ngs/input.sam > input.bz2 )
# import sys, bz2

# # Recover data in memory and decompress
# bzstring = ""
# for line in sys.stdin:
#     bzstring += line
# data = bz2.decompress(bzstring)
# del bzstring

# EOL = "\n"
# for line in data.split(EOL):

########################
# In case of a BAM file
# http://pysam.readthedocs.org/en/latest/api.html

# Decode BAM binary format
import pysam

# Cycle current streaming data
samstream = pysam.AlignmentFile("-", "rb")

# Iterable stream
for obj in samstream:

    # Get data
    line = obj.__str__()
    pieces = line.split(TAB)

    # Use data
    mystart = int(pieces[3])
    myseq = pieces[9]
    # Chr is an integer called "tid" (target identifier)
    # This has to map with references (the chromosome list inside bam header)
    mychr = samstream.getrname(int(pieces[2]))
    #print mychr, mystart, myseq

    # For each element with sequencing coverage
    for i in range(mystart, mystart + len(myseq)):
        results = [mychr+SEP+i.__str__(), "1"]
        print TAB.join(results)
