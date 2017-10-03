# Q1: Top 100 words - WikiData - Top100_MR (mrjob framework)

import re
from mrjob.job import MRJob
from heapq import heappush, nlargest


# First MR_job:
# Returns the counts of each of the words

WORD_RE = re.compile(r"\w+")

class MRWordCount(MRJob):

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)

    def reducer(self, word, counts):
        yield (word, sum(counts))


# Second Mr_job:
# Takes a collection of pairs (word, count) and filter for only the highest N e.g. 100

class MRTopN(MRJob):
    def mapper_init(self):
        self.h = []
        self.n = 100

    def mapper(self, word, counts):
        # Pushes the value of (counts, word) onto self.h
        heappush(self.h, (counts, word))

    def mapper_final(self):
        # Returns a list of the 100 largest elements from the self.h dataset
        largest = nlargest(self.n, self.h)
        for count, word in largest:
            yield (None, (count, word))

    def reducer_init(self):
        self.h_all = []
        self.n = 100

    def reducer(self, _, word_counts):
        # Pushes the value of counts onto self.h_all
        for words in word_counts:
            heappush(self.h_all, (words[0], words[1]))

    def reducer_final(self):
        # Bring it altogether
        largest = nlargest(self.n, self.h_all)
        words = [(word, int(count)) for count, word in largest]
        yield (None, words)


class MRTop100(MRJob):
    # Combine the word count and top N classes

    def steps(self):
        return MRWordCount().steps() + MRTopN().steps()


if __name__ == '__main__':
    MRTop100.run()


# TODO: Can use something like n = get_jobconf_value("my.job.settings.n")
# to get command line argument e.g. for n_umber of elements to put in top list