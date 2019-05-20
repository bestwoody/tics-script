import sys

KB = 1000
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
    return str(n)

def k2s(n):
    if long(n) / KB > 9:
        return str(long(n / KB)) + 'M'
    return str(n) + 'K'

def b2s(n):
    if long(n) / KB > 9:
        return k2s(long(n / KB))
    return str(n) + 'B'

def divb2s(a, b):
    if b == 0:
        return 'N/A'
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
