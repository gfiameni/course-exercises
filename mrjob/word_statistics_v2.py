import collections
import decimal
import itertools
import re

from mrjob.job import MRJob


NON_WORD_RE = re.compile(r'[^\w\-]')


class MRWordStatistics(MRJob):
    WordDescription = collections.namedtuple('WordDescription', 'count, index_in_line, is_capitalized')
    
    def reducer(self, word, word_descriptions):
        descriptor_totals = apply_elementwise_function(sum, word_descriptions)
        number_of_word_occurrences = descriptor_totals[0]
        average_index_and_capitalization = [total / decimal.Decimal(number_of_word_occurrences)
                                        for total in descriptor_totals[1:]]
        yield word, [number_of_word_occurrences] + average_index_and_capitalization
    
    def mapper(self, key, line):
        for index, word in enumerate(line.split()):
            word_without_symbols = NON_WORD_RE.sub('', word)
            word_description = (1, index, int(word_without_symbols.istitle()))
            yield word_without_symbols.lower(), word_description

def apply_elementwise_function(function, iterables):
    matching_elements = itertools.izip(*iterables)
    return map(function, matching_elements)

if __name__ == '__main__':
    MRWordStatistics.run()