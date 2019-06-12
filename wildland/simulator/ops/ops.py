import base

class Op:
    name = 'none'

class Write(Op):
    name = 'write'
    def __init__(self, k, thread_id):
        self.k = k
        self.thread_id = thread_id

class Read(Op):
    name = 'read'
    def __init__(self, k, thread_id):
        self.k = k
        self.thread_id = thread_id

class ScanRange(Op):
    name = 'scan range'
    # [begin, end)
    def __init__(self, begin, end, show_detail, thread_id = 0):
        self.begin = begin
        self.end = end
        self.show_detail = show_detail
        self.thread_id = thread_id

class ScanAll(Op):
    name = 'scan all'
    def __init__(self, show_detail, thread_id = 0):
        self.show_detail = show_detail
        self.thread_id = thread_id

class Pattern:
    def __call__(self):
        return Op()

    def str(self, indent):
        raise
