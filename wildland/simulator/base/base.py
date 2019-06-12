import sys
import decimal

KB = 1024
MB = KB * KB
GB = MB * KB

def enum(**enums):
    return type('Enum', (), enums)

class Limit:
    minint = -sys.maxint - 1
    maxint = sys.maxint

class Sorted(enum(none = 0, asc = 0x01, desc = 0x02, both = 0x03)):
    @staticmethod
    def str(n):
        if n == Sorted.both:
            return 'both'
        elif n == Sorted.asc:
            return 'asc'
        elif n == Sorted.desc:
            return 'desc'
        else:
            return 'none'

def i2s(n):
    if n == Limit.minint:
        return '-inf'
    if n == Limit.maxint:
        return '+inf'
    if n == Limit.maxint - 1:
        return '+inf-1'
    if n == Limit.minint + 1:
        return '-inf+1'
    return str(n)

def k2s(n):
    if long(n) / KB > 9:
        return str(long(n / KB)) + 'M'
    return str(n) + 'K'

def b2s(n):
    if long(n) / KB > 9:
        return k2s(long(n / KB))
    return str(long(n)) + 'B'

def div(a, b):
    if b == 0:
        return '-'
    return '%.1f' % (a / b)

def divb2s(a, b):
    if b == 0:
        return '-'
    return b2s(a / b)

def obj_id(obj):
    return '@' + str(id(obj))[-4:]

def upper_bound(array, k):
    first = 0
    left = len(array) - 1
    while left > 0:
        half = left >> 1
        middle = half + first
        if array[middle] > k:
            left = half
        else:
            first = middle + 1
            left -= half + 1
    return first

def locate_index_in_size_list(sizes, i):
    sum = 0
    j = 0
    while j < len(sizes):
        size = sizes[j]
        if i < sum + size:
            break
        sum += size
        j += 1
    return j, sum

def display(msgs, out = sys.stdout):
    if isinstance(msgs, list) or isinstance(msgs, tuple):
        map(lambda msg: out.write(str(msg)), msgs)
    else:
        out.write(str(msgs))

class AggAvg:
    def __init__(self):
        self.sum = 0
        self.count = 0

    def add(self, v):
        self.sum += v
        self.count += 1

    def empty(self):
        return self.count == 0

    def avg(self):
        return (self.count != 0) and (decimal.Decimal(self.sum) / self.count) or 0

if __name__ == '__main__':
    def test_locate_index_in_size_list(sizes, res):
        for i in range(0, len(res)):
            j, _ = locate_index_in_size_list(sizes, i)
            assert j == res[i]
    test_locate_index_in_size_list([2, 2, 5], [0, 0, 1, 1, 2, 2, 2])
    test_locate_index_in_size_list([1, 1, 1], [0, 1, 2, 3])
