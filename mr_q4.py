# Q4 Link statistics - WikiData - LinkStatistics_MR (mrjob framework)

from mrjob.job import MRJob
from mrjob.step import MRStep
from math import sqrt
import xml.etree.ElementTree as et
import re
from heapq import heappush, heapreplace
from mwparserfromhell import parse
from random import random

RE_WORD = re.compile(r'\w+')
RE_START = re.compile('.*<page>.*')
RE_END = re.compile('.*</page>.*')

LENGTH_RES = 2000


class LinkStatistics_MR(MRJob):
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


    def mapper_xml_init(self):
        self.sum_x = 0
        self.sum_x2 = 0
        self.pagecount = 0
        self.res = []
        self.k = LENGTH_RES


    def mapper_xml(self, _, page):
        try:
            root = et.fromstring(page)
        except:
            root = None
        if root is not None:
            tag_and_text = [(x.tag, x.text) for x in root.getiterator()]
            for tag, text in tag_and_text:
                try:
                    if (tag == 'text' and text):
                        hell_filter = parse(text)
                        wikilinks = hell_filter.filter_wikilinks()
                        x = len(set(wikilinks))
                        rand_num = random()
                        self.pagecount += 1
                        self.sum_x += x
                        self.sum_x2 += x*x
                        if len(self.res) < self.k:
                            heappush(self.res, (rand_num, x))
                        elif rand_num > self.res[0][0]:
                            heapreplace(self.res, (rand_num, x))
                except:
                    pass


    def mapper_xml_final(self):
        yield ('mean_and_deviation', (self.sum_x, self.sum_x2, self.pagecount))
        for rand_num, x in self.res:
            yield ('reservoir', (rand_num, x))


    def reducer_compute_stats(self, key, vals):
        if key == 'mean_and_deviation':
            N = 0
            sum_x = 0.0
            sum_x_sq = 0.0
            for val in vals:
                x, x_sq, x_num = val
                N += x_num
                sum_x += x
                sum_x_sq += x_sq
            N = float(N)
            mean = sum_x / N
            std_dev = sqrt(sum_x_sq / N - mean*mean)
            yield ('count', int(N))
            yield ('mean', mean)
            yield ('std dev', std_dev)
        elif key == 'reservoir':
            counts = [x for rand_num, x in vals]
            counts.sort()
            yield ('25th percentile', float(counts[int(round(len(counts) * 0.25) - 1)]))
            yield ('median', float(counts[int(round(len(counts) * 0.5) - 1)]))
            yield ('75th percentile', float(counts[int(round(len(counts) * 0.75) - 1)]))


    def steps(self):
        return [
            MRStep(mapper_init  = self.string_mapper_init,
                   mapper       = self.string_mapper,
                   reducer      = self.string_reducer),
            MRStep(mapper_init  = self.mapper_xml_init,
                   mapper       = self.mapper_xml,
                   mapper_final = self.mapper_xml_final,
                   reducer      = self.reducer_compute_stats)]


if __name__ == '__main__':
    LinkStatistics_MR.run()
