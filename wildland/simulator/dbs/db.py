import base
import hw

class DB:
    def __init__(self, hw, auto_compact = True):
        self.hw = hw
        self.auto_compact = auto_compact
        self.write_k_num = 0

    def write(self, k):
        self.write_k_num += 1

    def current_write_amplification(self):
        if self.write_k_num == 0:
            return 0.0
        return float(self.hw.total_write_k_num()) / self.write_k_num

    def manual_compact(self):
        pass

    def read(self, k, indent = 0, show_id = True):
        return ['(unimpl)', 'read ', k]

    def scan_range(self, begin, end, indent = 0, show_id = True):
        return ['(unimpl)', 'scan from ', begin, ' to ', end]

    def scan_all(self, indent = 0, show_id = True, show_detail = True):
        return ['(unimpl)', 'scan all']

class Seg:
    def __init__(self, min = base.Limit.maxint, max = base.Limit.minint, sorted = base.Sorted.both):
        self.min = min
        self.max = max
        self.size = 0
        self.sorted = sorted

    def write(self, k, split_min_size):
        if k < self.min:
            if self.size == 0:
                pass
            elif self.sorted == base.Sorted.desc or self.sorted == base.Sorted.both:
                self.sorted = base.Sorted.desc
            else:
                if self.size >= split_min_size:
                    return False
                self.sorted = base.Sorted.none
            self.min = k
        if k > self.max:
            if self.size == 0:
                pass
            elif self.sorted == base.Sorted.asc or self.sorted == base.Sorted.both:
                self.sorted = base.Sorted.asc
            else:
                if self.size >= split_min_size:
                    return False
                self.sorted = base.Sorted.none
            self.max = k
        self.size += 1
        return True

    def str(self, ref_number, show_id = False):
        id = lambda obj, name: show_id and name + base.obj_id(obj) or name
        size = (ref_number > 1) and '(' + str(self.size) + '/' + str(ref_number) + ')' or '=' + str(self.size)
        msg = [
            '{', id(self, 'seg:'), ' size', size, ' [', base.i2s(self.min), ', ',
            base.i2s(self.max), ']', ' ', base.Sorted.str(self.sorted), '}'
        ]
        return msg

class Dist:
    def __init__(self, seg_min = base.Limit.maxint):
        self.segs = [Seg()]
        self._curr = self.segs[-1]
        self.size = 0
        self.min = base.Limit.maxint
        self.max = base.Limit.minint
        self._seg_min = seg_min

    def write(self, k):
        written = self._curr.write(k, self._seg_min)
        if not written:
            self.segs.append(Seg())
            self._curr = self.segs[-1]
            written = self._curr.write(k, self._seg_min)
        assert(written)

        if len(self.segs) == 1:
            self.min = self._curr.min
            self.max = self._curr.max
        else:
            if k < self.min:
                self.min = k
            if k > self.max:
                self.max = k
        self.size += 1

    def is_desc(self):
        min = base.Limit.maxint
        for seg in self.segs:
            if seg.min > min:
                return False
            else:
                min = seg.min
            if seg.sorted != base.Sorted.desc and seg.sorted != base.Sorted.both:
                return False
        return True

    def is_asc(self):
        max = base.Limit.minint
        for seg in self.segs:
            if seg.max < max:
                return False
            else:
                max = seg.max
            if seg.sorted != base.Sorted.asc and seg.sorted != base.Sorted.both:
                return False
        return True

    def intersect(self, dist):
        if self.max <= dist.min or self.min >= dist.max:
            return False
        return True

    def str(self, ref_number, show_id = True):
        id = lambda obj, name: show_id and name + base.obj_id(obj) or name
        msg = [id(self, 'dist'), ' ']
        for seg in self.segs:
            msg += seg.str(ref_number, show_id)
        return msg

class File:
    def __init__(self, dist):
        self.dist = dist
        self.partitions = set()

    def str(self, indent = 0, show_id = True):
        id = lambda obj, name: show_id and name + base.obj_id(obj) or name
        msg = [' ' * indent, 'file', id(self, ''), '\n']
        msg += [' ' * (indent + 2), 'ref partitions: ', len(self.partitions)]
        for partition in self.partitions:
            msg += [' ', id(partition, '')]
        msg += ['\n', ' ' * (indent + 2)] + self.dist.str(len(self.partitions), show_id) + ['\n']
        return msg

class SegCache:
    def __init__(self, seg_min = base.Limit.maxint):
        self.dist = Dist(seg_min)
        self._size = 0
        self.size = lambda: self._size

    def write(self, k):
        self.dist.write(k)
        self._size += 1

    def str(self, show_id = True, show_detail = True):
        id = lambda obj, name: show_id and name + base.obj_id(obj) or name
        res = [id(self, 'cache:'), ' size=', self._size, ' ']
        if show_detail:
            res += self.dist.str(1, show_id)
        return res
