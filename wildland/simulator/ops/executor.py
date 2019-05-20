import sys
import base
import ops

def display(msgs, out = sys.stdout):
    if isinstance(msgs, list):
        map(lambda msg: out.write(str(msg)), msgs)
    else:
        out.write(str(msgs))

def execute_op(db, x, display_func = display):
    funcs = {
        ops.Write.name: lambda x: db.write(x.k),
        ops.ScanAll.name: lambda _: db.scan_all(2, x.show_detail, x.show_detail),
    }
    return funcs[x.name](x) or []

def execute_pattern(db, pattern, display_func = display):
    res = []
    while True:
        x = pattern()
        if not x:
            break
        res += execute_op(db, x, display_func) or []

def execute_one(db, target, display_func = display):
    if isinstance(target, ops.Pattern):
        return execute_pattern(db, target, display_func) or []
    elif isinstance(target, ops.Op):
        return execute_op(db, target, display_func) or []
    elif callable(target):
        return target() or []
    else:
        raise Exception('execute: unsupported type: ' + type(target).__name__)

def execute(db, targets, display_func = display):
    if isinstance(targets, list) or isinstance(targets, tuple):
        res = []
        for target in targets:
            res += execute_one(db, target, display_func) or []
    else:
        return execute_one(db, targets, display_func) or []
