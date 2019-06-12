import random
import base
import ops

class PatternAppend(ops.Pattern):
    def __init__(self, times, is_write, step = 1, base = 0, thread_id = 0):
        self._times = times
        self._create_op = is_write and ops.Write or ops.Read
        self._name = 'Append' + (is_write and 'Write' or 'Read')
        self._step = step
        self._base = base
        self._thread_id = thread_id
        self._k = base
        self._counter = times
        self.min = lambda: base
        self.max = lambda: base + times - 1

    def __call__(self):
        if self._times > 0 and self._counter <= 0:
            return None
        res = self._create_op(self._k, self._thread_id)
        self._k += self._step
        self._counter -= 1
        return res

    def str(self, indent):
        res = ' ' * indent + self._name + '('
        if self._times > 0:
            res += 'count:' + str(self._times) + ', '
        res += 'base:' + str(self._base) + ', step:' + str(self._step)
        res += ', thread_id:' + str(self._thread_id) + ')\n'
        return res

class PatternRand(ops.Pattern):
    def __init__(self, times, is_write, min = 0, max = base.Limit.maxint, thread_id = 0):
        self._times = times
        self._create_op = is_write and ops.Write or ops.Read
        self._name = 'Rand' + (is_write and 'Write' or 'Read')
        self._min = min
        self._max = max
        self._thread_id = thread_id
        self._counter = times
        self.min = lambda: self._min
        self.max = lambda: self._max

    def __call__(self):
        if self._times > 0 and self._counter <= 0:
            return None
        k = random.randint(self._min, self._max)
        res = self._create_op(k, self._thread_id)
        self._counter -= 1
        return res

    def str(self, indent):
        res =' ' * indent + self._name + '('
        if self._times > 0:
            res += 'count:' + str(self._times) + ', '
        res += 'min:' + str(self._min) + ', max:' + str(self._max)
        res += ', thread_id:' + str(self._thread_id) + ')\n'
        return res

class PatternRandScanRange(ops.Pattern):
    def __init__(self, times, min = 0, max = base.Limit.maxint, thread_id = 0, scale = 0.1, show_detail = True):
        self._times = times
        self._min = min
        self._max = max
        self._length = long((self._max - self._min) * scale)
        self._length_delta = long(self._length * scale)
        self._thread_id = thread_id
        self._scale = scale
        self._show_detail = show_detail
        self._counter = times
        self.min = lambda: self._min
        self.max = lambda: self._max

    def __call__(self):
        if self._times > 0 and self._counter <= 0:
            return None
        k = random.randint(self._min - self._length, self._max + self._length)
        length_delta = random.randint(-self._length_delta, self._length_delta)
        res = ops.ScanRange(k, k + self._length + length_delta, self._show_detail, self._thread_id)
        self._counter -= 1
        return res

    def str(self, indent):
        res =' ' * indent + 'RandScanRange('
        if self._times > 0:
            res += 'count:' + str(self._times) + ', '
        res += 'min:' + str(self._min) + ', max:' + str(self._max) + ', scale:' + str(self._scale)
        res += ', thread_id:' + str(self._thread_id) + ')\n'
        return res

class PatternRoundRobin(ops.Pattern):
    def __init__(self, times, children):
        self._times = times
        self._original = list(children)
        self._children = list(children)
        self._p = 0
        self._counter = times
        self.min = lambda: min(map(lambda x: x.min(), self._children))
        self.max = lambda: max(map(lambda x: x.max(), self._children))

    def __call__(self):
        if self._times > 0 and self._counter <= 0:
            return None
        if len(self._children) <= 0:
            return None
        if self._p >= len(self._children):
            self._p = 0
        res = self._children[self._p]()
        if not res:
            self._children.pop(self._p)
            return self()
        self._p += 1
        self._counter -= 1
        return res

    def str(self, indent):
        res = ' ' * indent + 'RoundRobin(children:' + str(len(self._original))
        if self._times > 0:
            res += ', count:' + str(self._times)
        res += ')\n'
        for child in self._original:
            res += child.str(indent + 2)
        return res

class PatternChance(ops.Pattern):
    def __init__(self, times, children, chances = None):
        self._times = times
        self._children = list(children)
        self._chances = chances and list(chances) or map(lambda _: 1, children)
        assert len(self._chances) == len(self._children)
        self._sum_chance = sum(self._chances)
        self._counter = times
        self.min = lambda: min(map(lambda x: x.min(), self._children))
        self.max = lambda: max(map(lambda x: x.max(), self._children))

    def __call__(self):
        if self._times > 0 and self._counter <= 0:
            return None
        i = random.randint(0, self._sum_chance - 1)
        child, _ = base.locate_index_in_size_list(self._chances, i)
        res = self._children[child]()
        self._counter -= 1
        return res

    def str(self, indent):
        res = ' ' * indent + 'Chance(children:' + str(len(self._children))
        if self._times > 0:
            res += ', count:' + str(self._times)
        res += ')\n'
        for i in range(0, len(self._children)):
            res += ' ' * (indent + 2) + ('%2.1f%%\n' % (self._chances[i] * 100 / float(self._sum_chance)))
            res += self._children[i].str(indent + 4)
        return res
