# Calculate the average tips to revenue ratio of the drivers for different pickup locations in sorted format.

from mrjob.job import MRJob

class AverageTipsToRevenueRatio(MRJob):

    def mapper(self, _, line):
        # Skip the header line
        if not line.startswith('VendorID'):
            fields = line.split(',')
            pickup_location_id = fields[7]
            total_amount = float(fields[16])
            tip = float(fields[13])
            yield pickup_location_id, (tip, total_amount)

    def combiner(self, pickup_location_id, tip_amount):
        total_tip = 0
        total_amount = 0
        for tip, amount in tip_amount:
            total_tip += tip
            total_amount += amount
        yield pickup_location_id, (total_tip, total_amount)

    def reducer(self, pickup_location_id, tip_amount):
        total_tip = 0
        total_amount = 0
        for tip, amount in tip_amount:
            total_tip += tip
            total_amount += amount
        average_tip_to_revenue_ratio = total_tip / total_amount
        yield pickup_location_id, average_tip_to_revenue_ratio

if __name__ == '__main__':
    AverageTipsToRevenueRatio.run()