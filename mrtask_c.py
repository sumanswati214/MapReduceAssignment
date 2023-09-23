# What are the different payment types used by customers and their count? The final results should be in a sorted format.

from mrjob.job import MRJob
from mrjob.step import MRStep

class CountPaymentType(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer),
            MRStep(reducer=self.reducer_sort_by_count),
            MRStep(reducer=self.final_reducer)
        ]

    def mapper(self, _, line):
        # Skip the header line
        if not line.startswith('VendorID'):
            fields = line.split(',')
            payment_type = fields[9]
            yield payment_type, 1

    def combiner(self, payment_type, count):
        yield payment_type, sum(count)

    def reducer(self, payment_type, count):
        yield payment_type, sum(count)

    def reducer_sort_by_count(self, payment_type, count):
        yield None, (sum(count), payment_type)

    def final_reducer(self, _, sorted_results):
        for count, payment_type in sorted(sorted_results, reverse=True):
            yield payment_type, count

if __name__ == '__main__':
    CountPaymentType.run()