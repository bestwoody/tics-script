import copy

class Task:
    def __init__(self, resource):
        self._curr = resource
        self._children = {}
        self.__getitem__ = self._children.__getitem__

    def new_stage(self, name):
        assert not self._children.has_key(name)
        resource = copy.deepcopy(self._curr)
        resource.reset()
        self._curr = resource
        self._children[name] = resource

    def __getattr__(self, name):
        attr = getattr(self._curr, name)
        assert not callable(attr), name
        return attr

    def elapsed_sec(self):
        if len(self._children) == 0:
            return self._curr.elapsed_sec()
        return sum(map(lambda child: child.elapsed_sec(), self._children.values()))

    def total_read_bytes(self):
        if len(self._children) == 0:
            return self._curr.disk.total_read_bytes()
        return sum(map(lambda child: child.disk.total_read_bytes(), self._children.values()))

    def total_write_bytes(self):
        if len(self._children) == 0:
            return self._curr.disk.total_write_bytes()
        return sum(map(lambda child: child.disk.total_write_bytes(), self._children.values()))

    def total_write_k_num(self):
        if len(self._children) == 0:
            return self._curr.disk.total_write_k_num()
        return sum(map(lambda child: child.disk.total_write_k_num(), self._children.values()))
