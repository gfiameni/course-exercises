""" Matrix matrix multiplication 

Input data should be in the form of 

i j     valuek
i j+1   valuek+1
i+1 j   valuek+2
i+1 j+1 value k+3


--verbose
"""

import sys
import random
import numpy
import pickle

from mrjob.job import MRJob
from mrjob.compat import get_jobconf_value
import os


class MatMult(MRJob):

    def configure_options(self):
        super(MatMult, self).configure_options()
        self.add_passthrough_option('--A-matrix', default='A', 
            dest='Amatname')

    def parsemat(self):
        """ Return 1 if this is the A matrix, otherwise return 2"""
        fn = get_jobconf_value('map.input.file')
        if self.options.Amatname in fn:
            return 1
        else:
            return 2

    def emit_values(self, _, line):
		
        # sys.stderr.write("MAPPER INPUT: ({0})\n".format(line))

        mtype = self.parsemat() 
        a, b, v = line.split()

        v = float(v)
        
        if mtype == 1:
            i = int(a)
            j = int(b)
            yield j, (0, i, v)
        else:
            j = int(a)
            k = int(b)
            yield j, (1, k, v)

    def multiply_values(self, j, values):
        values_from1 = []
        values_from2 = []
        
        # sys.stderr.write("REDUCER INPUT: ({0},{1})\n".format(j, values))
        
        for v in values:
            if v[0] == 0:
                values_from1.append(v)
            elif v[0] == 1:
                values_from2.append(v)
   
        for (m, i, v1) in values_from1:
            for (m, k, v2) in values_from2:
                yield (i, k), v1*v2

    def identity(self, k, v):
        yield k, v

    def add_values(self, k, values):
        yield k, sum(values)


    def steps(self):
        return [self.mr(mapper=self.emit_values,
                        reducer=self.multiply_values),
                self.mr(mapper=self.identity,
                        reducer=self.add_values)]

if __name__ == '__main__':
    MatMult.run()
