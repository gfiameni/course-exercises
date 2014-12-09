""" A bioinformatic MapReduce """

###############################################
import sys, re, time

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import PickleProtocol

data = {}
SEP = ":"

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
            mychr = pieces[2]   #[3:]
            mystart = int(pieces[3])
            myseq = pieces[9]

            # More handling: work on an ordable chromosome code
            tmp = mychr[3:]
            try:
                int(tmp)
                code = tmp.zfill(2)
            except ValueError:
                code = ord(tmp).__str__()
            #myqc = pieces[10]

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

#could be used to skip bad sequencing
#could be used to skip bad sequencing
                #current_qc = myqc[mypos]
#could be used to skip bad sequencing
#could be used to skip bad sequencing

                label = code + SEP + i.__str__() + SEP + mychr
                current_letter = myseq[mypos]

                yield label, 1
                yield label + SEP + current_letter, 1

            # print mychr, mystart.__str__(), mystop.__str__(), \
            #     len(myseq), (mystop - mystart).__str__()

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
