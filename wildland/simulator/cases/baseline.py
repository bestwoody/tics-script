import ops

def gen_rw_baseline_pattern(name, count, scale_count, is_write, thread_id_base):
    pattern = None
    if name == 'append':
        pattern = ops.PatternAppend(count, is_write)
    elif name == 'reversed_append':
        pattern = ops.PatternAppend(count, is_write, -1, scale_count)
    elif name == 'uniform':
        pattern = ops.PatternRand(count, is_write, 0, scale_count * 10)
    elif name == 'multi_append':
        pattern = ops.PatternRoundRobin(count, [
            ops.PatternAppend(0, is_write, 1, scale_count * 0, thread_id_base + 0),
            ops.PatternAppend(0, is_write, 1, scale_count * 1, thread_id_base + 1),
            ops.PatternAppend(0, is_write, 1, scale_count * 2, thread_id_base + 2),
            ops.PatternAppend(0, is_write, 1, scale_count * 3, thread_id_base + 3)])
    elif name == 'hotzone':
        base = 2 * scale_count
        pattern = ops.PatternChance(count, [
            ops.PatternRand(0, is_write, base, base + scale_count),
            ops.PatternRand(0, is_write, 0, scale_count * 10)], [95, 5])
    elif name == 'hotkeys':
        base = 2 * scale_count
        hot_size = max(scale_count / 100, 10)
        pattern = ops.PatternChance(count, [
            ops.PatternRand(0, is_write, base, base + 10),
            ops.PatternRand(0, is_write, 0, scale_count * 10)], [90, 10])
    return pattern

def gen_write_baseline_pattern(name, count):
    return gen_rw_baseline_pattern(name, count, count, True, 10)

def gen_read_baseline_pattern(name, count):
    return gen_rw_baseline_pattern(name, count, count, False, 10)

def gen_oltp_baseline_pattern(name, count):
    return ops.PatternChance(count, [
        gen_rw_baseline_pattern(name, 0, count, True, 10),
        gen_rw_baseline_pattern(name, 0, count, False, 50)], [20, 80])

def gen_olap_baseline_pattern(name, count):
    write_pattern = gen_write_baseline_pattern(name, count)
    mix_pattern = ops.PatternChance(count, [
        ops.PatternRandScanRange(0, write_pattern.min(), write_pattern.max(), 0, 0.1),
        gen_rw_baseline_pattern(name, 0, count, True, 10)], [80, 20])
    return write_pattern, mix_pattern
