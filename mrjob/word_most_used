from mrjob.job import MRJob


class MRWordCount(MRJob):

    def mapper(self, key, line):
        for word in line.split(' '):
			yield word.lower(),1

    def reducer(self, word, occurrences):
        yield word, sum(occurrences)

if __name__ == '__main__':
    MRWordCount.run()
