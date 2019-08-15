import sys
import base
import ops

def execute_op(db, x, op_funcs = None):
    func = op_funcs and (x.name in op_funcs) and op_funcs[x.name]
    if not func:
        funcs = {
            ops.Write.name: lambda x: db.write(x.k, x.thread_id),
            ops.Read.name: lambda x: db.read(x.k, x.thread_id),
            ops.ScanRange.name: lambda x: db.scan_range(x.begin, x.end, x.show_detail, x.thread_id),
            ops.ScanAll.name: lambda x: db.scan_all(2, x.show_detail, x.show_detail),
        }
        func = funcs[x.name]

    # Extract msg in result for display
    res = func(x)
    if isinstance(res, tuple):
        res = res[0]
    else:
        res = res or []
    return res

def execute_pattern(db, pattern, op_funcs = None):
    res = []
    while True:
        x = pattern()
        if not x:
            break
        res += execute_op(db, x, op_funcs) or []

def execute_one(db, target, op_funcs = None):
    if isinstance(target, ops.Pattern):
        return execute_pattern(db, target, op_funcs) or []
    elif isinstance(target, ops.Op):
        return execute_op(db, target, op_funcs) or []
    elif callable(target):
        return target() or []
    else:
        raise Exception('execute: unsupported type: ' + type(target).__name__)

def execute(db, targets, op_funcs = None):
    if isinstance(targets, list) or isinstance(targets, tuple):
        res = []
        for target in targets:
            res += execute_one(db, target, op_funcs) or []
    else:
        return execute_one(db, targets, op_funcs) or []

class ElapsedAvg:
    def __init__(self, db):
        self.db = db
        self.read_avg_sec = base.AggAvg()
        self.scan_range_avg_sec = base.AggAvg()
        self.scan_range_avg_output = base.AggAvg()

        ops_funcs = {
            ops.Read.name: lambda x: db.read(x.k, x.thread_id),
            ops.ScanRange.name: lambda x: db.scan_range(x.begin, x.end, x.show_detail, x.thread_id),
        }

        if db.env.config.calculate_elapsed:
            def do_read(x):
                msg, sec = db.read(x.k, x.thread_id)
                self.read_avg_sec.add(sec)
                return msg, sec
            ops_funcs[ops.Read.name] = do_read
            def do_scan_range(x):
                msg, size, sec = db.scan_range(x.begin, x.end, x.show_detail, x.thread_id)
                self.scan_range_avg_sec.add(sec)
                self.scan_range_avg_output.add(size)
                return msg, size, sec
            ops_funcs[ops.ScanRange.name] = do_scan_range

        self.ops_funcs = ops_funcs

    def str(self, indent = 0):
        msg = []
        if not self.read_avg_sec.empty():
            msg += [' ' * indent, 'Read avg elapsed time (ms): ', '%.3f' % (self.read_avg_sec.avg() * 1000), '\n']
        if not self.scan_range_avg_sec.empty():
            msg += [
                ' ' * indent, 'Scan range avg elapsed time (ms): ',
                '%.3f' % (self.scan_range_avg_sec.avg() * 1000), '\n',
                ' ' * indent, 'Scan range avg output: ',
                base.b2s(self.scan_range_avg_output.avg() * self.db.env.config.kv_size), ', ',
                '%.0f' % self.scan_range_avg_output.avg(), ' kv\n']
        return msg

class Executor:
    def __init__(self, db, elapsed_agg = None):
        self.db = db
        self._elapsed_agg = elapsed_agg or ElapsedAvg(db)

    def exe(self, targets):
        return execute(self.db, targets, self._elapsed_agg.ops_funcs)

    def elapsed_str(self, indent = 0):
        return self._elapsed_agg.str(indent)
