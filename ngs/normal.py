import sys, re

data = {}
# e.g. platform_id 99 chr1 8673801 29 151M = 8673952 229 TCAACTACA_SEQUENCE
RE_CHR_COORDS = r'(chr[^\s]+)\s+([0-9]+)\s+'
RE_SKIP = r'[^\s]+\s+[^\s]+\s+[^\s]+\s+[^\s]+\s+'   #pos to skip
RE_LEN_SEQUENCE = r'([0-9\-]+)\s+([ACTGN]+)'
SAM_CONTENT = re.compile(RE_CHR_COORDS + RE_SKIP + RE_LEN_SEQUENCE)

##############################################

def seq_range(start=0, stop=1):
    if (start > stop):
        return range(stop, start+1)
    else:
        return range(start, stop+1)

def use_line(line=None):
    match = SAM_CONTENT.search(line)
    if match:
        mychr = match.group(1)
        mystart = int(match.group(2))
        # group3=len, may be negative (reverse strand)
        mystop = mystart + int(match.group(3))
        #seq = match.group(4)
    else:
        return

    for i in seq_range(mystart, mystop):
        key = mychr + ":" + i.__str__()
        if key in data:
            data[key] = data[key] + 1
        else:
            data[key] = 1

##############################################

if __name__ == '__main__':
    with open(sys.argv[1], 'r') as my_file:
        for line in my_file.readlines():
            use_line(line)
    for key in sorted(data):
        print key, data[key]

##############################################
