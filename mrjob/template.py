
""" A MrJob Template for our workshop users """

import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep

class MyMapReduce(MRJob):

    def mapper(self, _, line):
        ## Do something with line?
        # columns = line.split()
        # print columns
        yield "lines_number", 1

    def reducer(self, key, values):
        yield key, sum(values)

    def steps(self):
        # Use what you need
        return [MRStep( \
            #mapper_pre_filter='grep -v "^@[^\\s]\+"',
            mapper=self.mapper,
            reducer=self.reducer,
        )]

# Main
if __name__ == "__main__":
    MyMapReduce.run()
