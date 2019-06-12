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
