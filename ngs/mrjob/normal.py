import sys, re #, time

data = {}
ALPHABET = "ACTGN"

# SAM format: http://samtools.sourceforge.net/samtools.shtml#5
# e.g. platform_id FLAG chr1 8673801 29 151M = 8673952 229 TCAACTACA_SEQUENCE
RE_CHR_COORDS = r'([^\s]+)\s+(chr[^\s]+)\s+([0-9]+)\s+'
RE_SKIP = r'[^\s]+\s+[^\s]+\s+[^\s]+\s+[^\s]+\s+[^\s]+\s+'   #pos to skip
RE_LEN_SEQUENCE = r'([' + ALPHABET + ']+)'
SAM_CONTENT = re.compile(RE_CHR_COORDS + RE_SKIP + RE_LEN_SEQUENCE)

##############################################

def use_line(line=None):

    # Find data and use it with regular expression
    match = SAM_CONTENT.search(line)
    if match:
        flag = int(match.group(1))
        mychr = match.group(2)
        mystart = int(match.group(3))
        seq = match.group(4)
    else:
        return

    # Handle STRAND: http://sourceforge.net/p/samtools/mailman/message/26793100/
    # Convert the flag to decimal, and check the bit in the 5th position from the right.
    # Solution: http://blog.nextgenetics.net/?e=18
    if flag & 16:
       #mystrand = 1
       mystop = mystart + len(seq)
    else:
       #mystrand = -1
       mystop = mystart
       mystart = mystop - len(seq)
    #print "\n", mystart, mystop, mystrand

    # PAIRED END??
    # As for the coverage question. One approach is to reconstitute the fragments from the read pairs and use these to compute the coverage.
    # https://www.biostars.org/p/7620/#7621

    # Save data in memory
    for i in range(mystart, mystop):
        key = mychr + ":" + i.__str__()
        current = i - mystart
        current_base = seq[current]
        #print i, current, current_base

        if key in data:
            # Add to coverage
            data[key]["coverage"] = data[key]["coverage"] + 1
        else:
            # Position init
            data[key] = {"coverage":1}
            for j in ALPHABET:
                data[key][j] = 0
        # Add to alphabet of current base
        data[key][current_base] = data[key][current_base] + 1

##############################################
#   MAIN

if __name__ == '__main__':

    # Work on each line in memory
    with open(sys.argv[1], 'r') as my_file:
        for line in my_file.readlines():
            use_line(line)

    # Print data obtained
    for key in sorted(data):
        print key, data[key]["coverage"], data[key]["A"], data[key]["C"], data[key]["T"], data[key]["G"], data[key]["N"]

##############################################
