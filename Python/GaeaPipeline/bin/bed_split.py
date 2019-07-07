from pybedtools import BedTool, Interval

class BedSplit(object):
    def __init__(self, bed_file):
        self.bed = BedTool(bed_file)


    def print_split_bed(self, split_num, prefix='out'):
        split_region_list = []
        for region in self.bed:
            pass


    def print_split_sort_bed(self):
        pass

split_num = 100

split_region_list = [[]*5]
print split_region_list
bed = BedTool('/Users/huangzhibo/workitems/10.testData/testPlatformTJ/bed/test.bed')


bed = BedTool(bed.sort().merge().window_maker(b=bed.fn, w=100))

bed.all_hits()

# x = BedTool().window_maker(genome='hg38', w=1000000)
bed.saveas('/Users/huangzhibo/workitems/10.testData/testPlatformTJ/bed/test_w100.bed')

split_num = bed.count() if bed.count() < split_num else split_num

print bed.count()/split_num

# print bed.split(10, 'out')

# print x

n = 0
for region in bed:
    # print region.length
    print str(region).strip()
    n += 1

print n

# print bed
# for l in
# bed:
#     print str(l)

    # Interval()
# bed.window_maker()
