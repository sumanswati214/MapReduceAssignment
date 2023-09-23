# Which pickup location generates the most revenue? 

from mrjob.job import MRJob
from mrjob.step import MRStep

class MostRevenuePickupLocation(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.final_reducer)
        ]

    def mapper(self, _, line):
        # Skip the header line
        if not line.startswith('VendorID'):
            fields = line.split(',')
            pickup_location_id = fields[7]
            total_amount = float(fields[16])
            yield pickup_location_id, total_amount

    def reducer(self, key, value):
        yield None, (sum(value), key)

    def final_reducer(self, _, max_total_amount):
        max_total_amount, pickup_location_id = max(max_total_amount)
        yield pickup_location_id, max_total_amount

if __name__ == '__main__':
    MostRevenuePickupLocation.run()