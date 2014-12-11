""" A bioinformatic MapReduce """

###############################################
import sys, re, time

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import PickleProtocol

SEP = ":"
# Formats for quality reading: http://j.mp/1zo30bt
# Illumina 1.8+ Phred+33,  raw reads typically (0, 41)
PHRED_INIT = 33
PHRED_MIN = 0
PHRED_MAX = 41
# What is the minimum quality?
PHRED_THOLD = 20

###############################################
class MRcoverage(MRJob):
    """ Implementation of a job MapReduce """

    # Optimization on internal protocols
    INTERNAL_PROTOCOL = PickleProtocol
    OUTPUT_PROTOCOL = PickleProtocol

    def mapper(self, _, line):
        """ Split data into keys and value """

        if line[0] == "@":
            yield "header", 1
        else:
            # Recover data
            pieces = line.split("\t")
            flag = int(pieces[1])
            mychr = pieces[2]
            mystart = int(pieces[3])
            myseq = pieces[9]

            # More handling: work on an ordable chromosome code
            tmp = mychr[3:]
            try:
                int(tmp)
                code = tmp.zfill(2)
            except ValueError:
                code = ord(tmp).__str__()
            myqc = pieces[10]

            # Handle STRAND:
            # Convert the flag to decimal, and check
            # the bit in the 5th position from the right.
            if flag & 16:       # Strand forward
               mystop = mystart + len(myseq)
            else:               # Strand reverse
               mystop = mystart + 1
               mystart = mystop - len(myseq)

            # For each piece of the sequence
            for i in xrange(mystart, mystop):
                mypos = i - mystart

                #tmp = myqc[mypos]
                #print "TEST ", tmp, ord(tmp), tmp.encode("ascii")
                #time.sleep(2)

                # Compute quality value
                # which should be in the range 0..40
                current_qc = ord(myqc[mypos]) - PHRED_INIT

                # It will be used to skip bad sequencing
                if current_qc < PHRED_MIN or current_qc > PHRED_MAX:
                    raise Exception("Wrong encoding 'Sanger' Phred33+...!\n" + \
                        "Found " +current_qc.__str__()+ " at " + \
                        mychr + " Pos " + i.__str__())
                if current_qc > PHRED_THOLD:
                    label = code + SEP + i.__str__() + SEP + mychr
                    current_letter = myseq[mypos]

                    yield label, 1
                    yield label + SEP + current_letter, 1

    def reducer(self, key, values):
        yield key, sum(values)

    def steps(self):
        """ Steps required from our MapReduce job """
        return [MRStep( \
            #mapper_pre_filter='grep -v "^@[^\\s]\+"',
            mapper=self.mapper,
            reducer=self.reducer,
        )]

##############################################
if __name__ == "__main__":
    MRcoverage.run()
