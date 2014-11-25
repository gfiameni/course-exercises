from mrjob.job import MRJob


class MRWordFrequencyCount(MRJob):

    # takes a key and a value as args (in this case, the key is 
    # ignored and a single line of text input is the value) and 
    # yields as many key-value pairs as it likes. 
    def mapper(self, _, line):
        yield "chars", len(line)
        yield "words", len(line.split())
        yield "lines", 1

    # takes a key and an iterator of values and also yields as 
    # many key-value pairs as it likes. (In this case, it sums 
    # the values for each key, which represent the numbers of 
    # characters, words, and lines in the input.)
    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRWordFrequencyCount.run()
