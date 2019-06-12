import decimal
import base
import ops

def write(db, pattern):
    kv_size = db.env.config.kv_size
    hw = db.env.hw
    hw.switch('write', True)

    old_k_num = db.k_num()
    ops.execute(db, pattern)

    k_num = db.k_num() - old_k_num
    write_bytes = base.b2s(hw.disk.total_write_k_num() * kv_size)
    write_kv = '%.0f' % hw.disk.total_write_k_num()
    read_bytes = base.b2s(long(hw.disk.total_read_k_num() * kv_size))
    read_kv = '%.0f' % hw.disk.total_read_k_num()
    sec = hw.elapsed_sec(kv_size)
    if sec > 0:
        perf_bytes = base.divb2s(k_num * kv_size, sec)
        perf_kv = base.div(k_num, sec)
    elif k_num > 0:
        perf_bytes = '(max)'
        perf_kv = '(max)'

    base.display([
        'Write data:\n', '  Pattern:\n',
        pattern.str(4),
        '  Elapsed time (ms): ', '%.3f' % (hw.elapsed_sec(kv_size) * 1000), '\n',
        '  Disk bytes write: ', write_bytes, ', ', write_kv, ' kv\n',
        '  Disk bytes read: ', read_bytes, ', ', read_kv, ' kv\n',
        '  Write performance: ', perf_bytes + '/s, ', perf_kv, ' kv/s\n',
        '  Write amplification (include WAL): ', '%.1f%%' % (db.current_write_amplification() * 100), '\n'])

    base.display(['  Load of:\n'])
    for k, v in hw.load(kv_size):
        base.display(['    ', k, ': ', v, '\n'])

def scan_all(db, verb_level, title):
    kv_size = db.env.config.kv_size
    hw = db.env.hw
    hw.switch('scan_call', True)
    hw.set_snapshot()
    scan_msg = ops.execute(db, ops.ScanAll(verb_level == 2))

    if verb_level > 0:
        base.display(['ScanAll(', title, ') simulating: ', db.partition_num(), ' partitions\n'])
        base.display(scan_msg)
        base.display('---\n')

    output_kv = '%.0f' % hw.api.total_output_k_num()
    write_bytes = base.b2s(hw.disk.total_write_k_num() * kv_size)
    write_kv = '%.0f' % hw.disk.total_write_k_num()
    read_bytes = base.b2s(long(hw.disk.total_read_k_num() * kv_size))
    read_kv = '%.0f' % hw.disk.total_read_k_num()

    sec = hw.elapsed_sec(kv_size)
    k_num = hw.api.total_output_k_num()
    if sec > 0:
        perf_bytes = base.divb2s(k_num * kv_size, sec)
        perf_kv = base.div(hw.api.total_output_k_num(), sec)
    elif k_num > 0:
        perf_bytes = '(max)'
        perf_kv = '(max)'
    else:
        perf_bytes = '0'
        perf_kv = '0'

    base.display([
        'ScanAll(', title, '):\n',
        '  Elapsed time (ms): ', '%.3f' % (hw.elapsed_sec(kv_size) * 1000), '\n',
        '  Disk bytes write: ', write_bytes, ', ', write_kv, ' kv\n',
        '  Disk bytes read: ', read_bytes, ', ', read_kv, ' kv\n',
        '  Partition count: ', db.partition_num(), '\n',
        '  File count: ', db.file_num(), '\n'
        '  Scan output: ', output_kv, 'kv\n',
        '  Scan performance(', title, '): ', perf_bytes + '/s, ', perf_kv, ' kv/s\n',
        '  Write amplification (include WAL): ', '%.1f%%' % (db.current_write_amplification() * 100), '\n'])

    base.display(['  Load of:\n'])
    for k, v in hw.load(kv_size):
        base.display(['    ', k, ': ', v, '\n'])

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
    hw = db.env.hw
    hw.switch(title, True)

    old_k_num = db.k_num()
    ops.execute(db, pattern, ops_funcs)

    base.display([
        title + ':\n', '  Pattern:\n',
        pattern.str(4)])

    k_num = db.k_num() - old_k_num
    output_kv = hw.api.total_output_k_num()
    write_bytes = base.b2s(hw.disk.total_write_k_num() * kv_size)
    write_kv = hw.disk.total_write_k_num()
    read_bytes = base.b2s(long(hw.disk.total_read_k_num() * kv_size))
    read_kv = hw.disk.total_read_k_num()
    sec = hw.elapsed_sec(kv_size)
    if sec > 0:
        perf_bytes = base.divb2s(decimal.Decimal(k_num + output_kv) * kv_size, sec)
        perf_kv = base.div(decimal.Decimal(k_num + output_kv), sec)
    elif k_num > 0:
        perf_bytes = '(max)'
        perf_kv = '(max)'
    else:
        perf_bytes = '0'
        perf_kv = '0'

    base.display([
        '---\n',
        '  Elapsed time (ms): ', '%.3f' % (hw.elapsed_sec(kv_size) * 1000), '\n',
        '  Disk bytes write: ', write_bytes, ', ', '%.0f' % write_kv, ' kv\n',
        '  Disk bytes read: ', read_bytes, ', ', '%.0f' % read_kv, ' kv\n',
        '  Partition count: ', db.partition_num(), '\n',
        '  File count: ', db.file_num(), '\n'
        '  Write count: ', '%.0f' % k_num, 'kv\n',
        '  Read output: ', '%.0f' % output_kv, 'kv\n',
        '  Summary(w+r) performance: ', perf_bytes + '/s, ', perf_kv, ' kv/s\n',
        '  Write amplification (include WAL): ', '%.1f%%' % (db.current_write_amplification() * 100), '\n'])

    if not read_avg_sec.empty():
        base.display(['  Read avg elapsed time (ms): ', '%.3f' % (read_avg_sec.avg() * 1000), '\n'])
    if not scan_range_avg_sec.empty():
        base.display(['  Scan range avg elapsed time (ms): ', '%.3f' % (scan_range_avg_sec.avg() * 1000), '\n'])
        base.display(['  Scan range avg output: ', base.b2s(scan_range_avg_output.avg() * kv_size), ', ',
            '%.0f' % scan_range_avg_output.avg(), ' kv\n'])

    kvs_hit_count, kvs_access_count = db.env.kvs_cache.hit_total()
    if kvs_access_count > 0:
        base.display(['  Kvs hit rate: ', kvs_hit_count, '/', kvs_access_count,
            ' = %.1f%%' % (float(kvs_hit_count) * 100 / kvs_access_count),
            ', size: ', db.env.kvs_cache.size(), ' kv\n'])

    base.display(['  Load of:\n'])
    for k, v in hw.load(kv_size):
        base.display(['    ', k, ': ', v, '\n'])
