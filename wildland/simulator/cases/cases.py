import base
import ops

def write_then_scan(db, kv_size, task, write_pattern, times, verb_level):
    task.new_stage('write')
    ops.execute(db, write_pattern)
    task.new_stage('scan')
    scan_msg = ops.execute(db, ops.ScanAll(verb_level == 2))

    ops.display(['Write pattern:\n', write_pattern.str(2)])
    ops.display([
        '---', '\n',
        'Total key-value count: ', db.write_k_num, '\n',
        'Write amplification (include WAL): ', db.current_write_amplification(), '\n',
        'Write bytes: ', base.b2s(task['write'].disk.total_write_k_num() * kv_size), '\n',
        'Write elapsed sec: ', task['write'].elapsed_sec(kv_size), '\n',
        'Write performance: ', base.divb2s(task['write'].disk.total_write_k_num() * kv_size,
        task['write'].elapsed_sec(kv_size)), '\n'])
    for k, v in task['write'].load(kv_size):
        ops.display(['Load of ', k, ': ', v * 100, '%\n'])
    if verb_level > 0:
        ops.display([
            '---', '\n',
            'Read simulating:', '\n'])
        ops.display(scan_msg)
    ops.display([
        '---', '\n',
        'Partition count: ', len(db.partitions), '\n',
        'Read bytes: ', base.b2s(task['scan'].disk.total_read_k_num() * kv_size), '\n',
        'Read elapsed sec: ', task['scan'].elapsed_sec(kv_size), '\n',
        'Read performance: ', base.divb2s(task['scan'].disk.total_read_k_num() * kv_size,
        task['scan'].elapsed_sec(kv_size)), '\n'])
    for k, v in task['scan'].load(kv_size):
        ops.display(['Load of ', k, ': ', v * 100, '%\n'])
