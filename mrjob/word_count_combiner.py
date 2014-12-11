from mrjob.job import MRJob


class MRWordCount2(MRJob):

    def mapper(self, key, line):
        for word in line.split(' '):
			yield word.lower(),1
	
	# Combiner step
    def combiner(self, word, occurrences):
        yield word, sum(occurrences)

    def reducer(self, word, occurrences):
        yield word, sum(occurrences)

if __name__ == '__main__':
    MRWordCount2.run()
