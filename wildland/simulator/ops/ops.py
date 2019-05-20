import base

class Op:
    name = 'none'

class Write(Op):
    name = 'write'
    def __init__(self, k):
        self.k = k

class Read(Op):
    name = 'read'
    def __init__(self, k):
        self.k = k

class ScanRange(Op):
    name = 'scan range'
    # [begin, end)
    def __init__(self, begin, end, show_detail):
        self.begin = end
        self.show_detail

class ScanAll(Op):
    name = 'scan all'
    def __init__(self, show_detail):
        self.show_detail = show_detail

class Pattern:
    def __call__(self):
        return Op()

    def str(self, indent):
        raise
