""" A bioinformatic MapReduce """

###############################################
import re, time
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import PickleProtocol

# e.g. platform_id 99 chr1 8673801 29 151M = 8673952 229 TCAACTACA_SEQUENCE
RE_CHR_COORDS = r'(chr[^\s]+)\s+([0-9]+)\s+'
RE_SKIP = r'[^\s]+\s+[^\s]+\s+[^\s]+\s+[^\s]+\s+'   #pos to skip
RE_LEN_SEQUENCE = r'([0-9\-]+)\s+([ACTGN]+)'
SAM_CONTENT = re.compile(RE_CHR_COORDS + RE_SKIP + RE_LEN_SEQUENCE)

def seq_range(start=0, stop=1):
    if (start > stop):
        return range(stop, start+1)
    else:
        return range(start, stop+1)

###############################################
class MRcoverage(MRJob):

    INTERNAL_PROTOCOL = PickleProtocol
    OUTPUT_PROTOCOL = PickleProtocol

    def mapper(self, _, line):

        match = SAM_CONTENT.search(line)
        if match:
            mychr = match.group(1)
            mystart = int(match.group(2))
            mystop = mystart + int(match.group(3))
            #seq = match.group(4)
        else:
            return

        for i in seq_range(mystart, mystop):
            key = mychr + ":" + i.__str__()
            yield key, 1

    def reducer(self, key, values):
        yield key, sum(values)

    def steps(self):
        return [ MRStep(
            #mapper_pre_filter='grep -v "^@[^\\s]\+"',
            mapper=self.mapper,
            reducer=self.reducer,
        ) ]

if __name__ == "__main__":
    MRcoverage.run()