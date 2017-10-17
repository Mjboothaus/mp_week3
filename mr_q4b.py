from mrjob.job import MRJob
#from mrjob.step import MRStep
from lxml import etree
#from heapq import heappush, heappop, nlargest
import re
import mwparserfromhell
import numpy as np

WORD_RE = re.compile(r"[\w']+")


class MRWordCount(MRJob):
    def mapper_init(self):
        self.buff = []
        self.rflag = False

    def mapper_get_words(self, _, line):
        if '<page>' in line:
            self.rflag = True

        if self.rflag:
            self.buff.append(line.rstrip())

        if '</page>' in line and self.rflag:
            self.rflag = False
            root = etree.fromstring(' '.join(self.buff))
            self.buff = []

            #      for elem in root.findall('.//text'):
            #        if type(elem.text) is not str:
            #          continue

            result = ''
            for element in root.iter():
                if element.tag == 'revision':
                    for child in element:
                        if child.tag == 'text':
                            if child.text:
                                result += ' ' + child.text
            wikicode = mwparserfromhell.parse(result)
            nlink = len(set(wikicode.filter_wikilinks()))
            yield (None, nlink)

    def reducer_count_words(self, _, nlinks):
        # send all (num_occurrences, word) pairs to the same reducer.
        for n in nlinks:
            yield None, n


# def print_stats(nlinks):
#     print 'mean:', np.mean(nlinks)
#     print 'std:', np.std(nlinks)
#     print 'median', np.median(nlinks)
#     print '25%:', np.percentile(nlinks, 25)
#     print '75%', np.percentile(nlinks, 75)
#    print '------------------------'

if __name__ == '__main__':
    MRWordCount.run()