import decimal
import base
import ops

def status_from_snapshot(db, indent = 0):
    kv_size = db.env.config.kv_size
    hw = db.env.hw

    k_num = db.write_k_num_from_snapshot()
    output_kv = hw.api.total_output_k_num()
    write_kv = hw.disk.total_write_k_num()
    read_kv = hw.disk.total_read_k_num()

    sec = hw.elapsed_sec(kv_size)

    def get_perf_str(n, sec):
        if sec > 0:
            perf_bytes = base.divb2s(n * kv_size, sec)
            perf_kv = base.div(n, sec)
        elif n > 0:
            perf_bytes = '(max)'
            perf_kv = '(max)'
        else:
            perf_bytes = '0'
            perf_kv = '0'
        return perf_bytes, perf_kv

    def add_line(res, *line):
        res.append(' ' * indent)
        res += list(line)
        res.append('\n')

    res = []
    add_line(res, 'Elapsed ms: ', '%.3f' % (hw.elapsed_sec(kv_size) * 1000))
    add_line(res, 'Write amplification include WAL: ', '%.1f%%' % (db.current_write_amplification() * 100))

    if k_num + output_kv + write_kv + read_kv > 0:
        add_line(res, 'IO:')
        if k_num > 0:
            add_line(res, '  DB write: ', k_num, ' kv')
        if output_kv > 0:
            add_line(res, '  DB read: ', '%.0f' % output_kv, ' kv')
        if write_kv > 0:
            add_line(res, '  Disk write: ', base.b2s(write_kv * kv_size), ', ', '%.0f' % write_kv, ' kv')
        if read_kv > 0:
            add_line(res, '  Disk read: ', base.b2s(read_kv * kv_size), ', ', '%.0f' % read_kv, ' kv')
    if k_num > 0:
        perf_bytes, perf_kv = get_perf_str(k_num, sec)
        add_line(res, 'Write performance: ', perf_bytes + '/s, ', perf_kv, ' kv/s')
    if output_kv > 0:
        perf_bytes, perf_kv = get_perf_str(output_kv, sec)
        add_line(res, 'Read performance: ', perf_bytes + '/s, ', perf_kv, ' kv/s')

    res += db.env.hw.load_str(kv_size, indent)

    _, kvs_cache_access_count = db.env.kvs_cache.hit_total()
    files_cache_mem = db.env.files_cache.mem_used()
    if kvs_cache_access_count + files_cache_mem > 0:
        add_line(res, 'Cache status')
        if kvs_cache_access_count > 0:
            res += db.env.kvs_cache.str(kv_size, db.k_num(), indent + 2)
        if files_cache_mem > 0:
            add_line(res, '  FilesCache size: ', '%0.f' % files_cache_mem, '/', '%0.f' % db.k_num(),
                ' kv = %.0f%% DB Size' % (float(files_cache_mem) * 100 / db.k_num()))

    return res

def write(db, pattern):
    db.set_snapshot('write')
    ops.execute(db, pattern)
    base.display(['Write simulating:\n  Pattern:\n', pattern.str(4), '  -\n'])
    base.display(status_from_snapshot(db, 2))

def scan_all(db, verb_level, title):
    db.set_snapshot('scan_all_' + title)
    scan_msg = ops.execute(db, ops.ScanAll(verb_level == 2))
    if verb_level > 0:
        base.display(['ScanAll(', title, ') simulating: ', db.partition_num(), ' partitions\n'])
        base.display(scan_msg)
        base.display('---\n')
    base.display(['ScanAll(', title, '):\n'])
    base.display(status_from_snapshot(db, 2))

def write_then_scan(db, pattern, verb_level):
    kv_size = db.env.config.kv_size

    write(db, pattern)
    base.display('---\n')
    scan_all(db, verb_level, '1st')
    if db.env.config.auto_compact:
        base.display('---\n')
        scan_all(db, verb_level, '2nd')

def mix_ops(db, pattern, verb_level, title):
    db.set_snapshot(title)
    executor = ops.Executor(db)
    executor.exe(pattern)
    base.display([title + ':\n', '  Pattern:\n', pattern.str(4), '  -\n'])
    base.display(status_from_snapshot(db, 2))
    base.display(executor.elapsed_str(2))
