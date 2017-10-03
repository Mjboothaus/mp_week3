# Q2: Top 100 words - WikiData - Top100_MR (mrjob framework)

from mrjob.job import MRJob
from mrjob.step import MRStep
import xml.etree.ElementTree as et
import re
# import mwparserfromhell
from mwparserfromhell import parse
from heapq import nlargest, heappush

RE_WORD = re.compile(r'\w+')

RE_START = re.compile('.*<page>.*')
RE_END = re.compile('.*</page>.*')

TEXT_TAG = 'text'


class Top100_MR(MRJob):
    def string_mapper_init(self):
        self.pageall = ''

    def string_mapper(self, _, line):
        self.pageall = self.pageall + line
        if RE_END.match(line):
            page = self.pageall
            self.pageall = ''
            if RE_START.match(page):
                yield (None, page)

    def string_reducer(self, _, pages):
        for page in pages:
            yield (None, page)

    def mapper_extract_xml_words(self, _, page):
        root = et.fromstring(page.encode('utf-8'))
        tag_and_text = [(x.tag, x.text) for x in root.getiterator()]
        for tag, text in tag_and_text:
            if (tag == TEXT_TAG and text):
                parse_filter = parse(text).strip_code()
                for parsedtext in parse_filter.filter_text():
                    for word in RE_WORD.findall(parsedtext.value):
                        yield (word.lower(), 1)

    def combiner_count_words(self, word, counts):
        # Sum words we've got to date
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        # Send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can use max() function
        yield (word, sum(counts))

    def heap_mapper_init(self):
        self.h = []
        self.n = 100

    def heap_mapper(self, word, counts):
        # Pushes the value of (counts, word) onto self.h
        heappush(self.h, (counts, word))

    def heap_mapper_final(self):
        # Returns a list of the 100 largest elements from self.h
        largest = nlargest(self.n, self.h)
        for count, word in largest:
            yield (None, (count, word))

    def heap_reducer_init(self):
        self.h_all = []
        self.n = 100

    def heap_reducer(self, _, word_counts):
        # Pushes the value of counts (words[0]), words[1]) onto self.h_all
        for words in word_counts:
            heappush(self.h_all, (words[0], words[1]))

    def heap_reducer_final(self):
        # Bring it altogether
        largest = nlargest(self.n, self.h_all)
        words = [(word, int(count)) for count, word in largest]
        yield (None, words)

    # Now override the default steps() method

    def steps(self):
        return [
            MRStep(mapper_init=self.string_mapper_init,
                   mapper=self.string_mapper,
                   reducer=self.string_reducer),
            MRStep(mapper=self.mapper_extract_xml_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(mapper_init=self.heap_mapper_init,
                   mapper=self.heap_mapper,
                   mapper_final=self.heap_mapper_final,
                   reducer_init=self.heap_reducer_init,
                   reducer=self.heap_reducer,
                   reducer_final=self.heap_reducer_final)]


if __name__ == '__main__':
    Top100_MR.run()
