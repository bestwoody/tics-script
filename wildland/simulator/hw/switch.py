import copy

class ResourceSwitch:
    def __init__(self, resource):
        self._curr = resource
        self._children = {}
        self.__getitem__ = self._children.__getitem__

        class Obj:
            pass
        self.summary = Obj()
        self.summary.disk = Obj()
        self.summary.disk.total_write_k_num = lambda: sum(map(lambda x: x.disk.total_write_k_num(), self._children.values()))

    def switch(self, name, remove_if_exists = False):
        exists = self._children.has_key(name)
        if exists:
            if remove_if_exists:
                self._children.pop(name)
            else:
                raise Exception('resource name: ' + name + ' exists')
        resource = copy.deepcopy(self._curr)
        resource.reset()
        self._curr = resource
        self._children[name] = resource

    def __getattr__(self, name):
        attr = hasattr(self._curr, name) and getattr(self._curr, name) or None
        attr = attr or self._children.has_key(name) and self._children[name]
        return attr
