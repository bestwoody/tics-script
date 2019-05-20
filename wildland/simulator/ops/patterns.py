import random
import base
import ops

class PatternAppend(ops.Pattern):
    def __init__(self, times, step = 1, base = 0):
        self._times = times
        self._step = step
        self._base = base
        self._k = base
        self._counter = times

    def __call__(self):
        if self._counter <= 0:
            return None
        res = ops.Write(self._k)
        self._k += self._step
        self._counter -= 1
        return res

    def str(self, indent):
        return ' ' * indent + 'Append(count:' + str(self._times) + \
            ', base:' + str(self._base) + ', step:' + str(self._step) + ')\n'

class PatternRand(ops.Pattern):
    def __init__(self, times, min = 0, max = base.Limit.maxint):
        self._times = times
        self._min = min
        self._max = max
        self._counter = times

    def __call__(self):
        if self._counter <= 0:
            return None
        k = random.randint(self._min, self._max)
        res = ops.Write(k)
        self._counter -= 1
        return res

    def str(self, indent):
        return ' ' * indent + 'Rand(count:' + str(self._times) + \
            ', min:' + str(self._min) + ', max:' + str(self._max) + ')\n'

class PatternRR(ops.Pattern):
    def __init__(self, children, times = -1):
        self._times = times
        self._original = list(children)
        self._children = list(children)
        self._p = 0
        self._counter = times

    def __call__(self):
        if self._times >= 0 and self._counter <= 0:
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
        res = ' ' * indent + 'RoundRobin(count:' + str(len(self._original)) + ')\n'
        for child in self._original:
            res += child.str(indent + 2)
        return res
