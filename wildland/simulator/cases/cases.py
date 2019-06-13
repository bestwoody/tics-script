import decimal
import base
import ops

# TODO: move `display` to each modules

def write(db, pattern):
    db.set_snapshot('write')
    ops.execute(db, pattern)
    base.display(['Write simulating:\n  Pattern:\n', pattern.str(4), '  -\n'])
    base.display(db.status_from_snapshot(2))

def scan_all(db, verb_level, title):
    db.set_snapshot('scan_all_' + title)
    scan_msg = ops.execute(db, ops.ScanAll(verb_level == 2))
    if verb_level > 0:
        base.display(['ScanAll(', title, ') simulating: ', db.partition_num(), ' partitions\n'])
        base.display(scan_msg)
        base.display('---\n')
    base.display(['ScanAll(', title, '):\n'])
    base.display(db.status_from_snapshot(2))

def write_then_scan(db, pattern, verb_level):
    kv_size = db.env.config.kv_size

    write(db, pattern)
    base.display('---\n')
    scan_all(db, verb_level, '1st')
    if db.env.config.auto_compact:
        base.display('---\n')
        scan_all(db, verb_level, '2nd')

def run_mix_ops(db, pattern, verb_level, title):
    ops_funcs = {
        ops.Read.name: lambda x: db.read(x.k, x.thread_id),
        ops.ScanRange.name: lambda x: db.scan_range(x.begin, x.end, x.show_detail, x.thread_id),
    }
    read_avg_sec = base.AggAvg()
    scan_range_avg_sec = base.AggAvg()
    scan_range_avg_output = base.AggAvg()
    if db.env.config.calculate_elapsed:
        def do_read(x):
            msg, sec = db.read(x.k, x.thread_id)
            read_avg_sec.add(sec)
            return msg, sec
        ops_funcs[ops.Read.name] = do_read
        def do_scan_range(x):
            msg, size, sec = db.scan_range(x.begin, x.end, x.show_detail, x.thread_id)
            scan_range_avg_sec.add(sec)
            scan_range_avg_output.add(size)
            return msg, size, sec
        ops_funcs[ops.ScanRange.name] = do_scan_range

    kv_size = db.env.config.kv_size

    db.set_snapshot(title)
    ops.execute(db, pattern, ops_funcs)
    base.display([title + ':\n', '  Pattern:\n', pattern.str(4), '  -\n'])
    base.display(db.status_from_snapshot(2))

    if not read_avg_sec.empty():
        base.display(['  Read avg elapsed time (ms): ', '%.3f' % (read_avg_sec.avg() * 1000), '\n'])
    if not scan_range_avg_sec.empty():
        base.display(['  Scan range avg elapsed time (ms): ', '%.3f' % (scan_range_avg_sec.avg() * 1000), '\n'])
        base.display(['  Scan range avg output: ', base.b2s(scan_range_avg_output.avg() * kv_size), ', ',
            '%.0f' % scan_range_avg_output.avg(), ' kv\n'])
