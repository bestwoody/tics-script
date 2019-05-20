import sys

import hw
import dbs
import ops
import cases

def lazydog_write_then_scan():
    if len(sys.argv) != 6:
        print 'usage: <bin> times partition-split-size seg-min-size, auto-compact verb-level\
            (0=none, 1=necessary, 2=all)'
        sys.exit(-1)

    times = int(sys.argv[1])
    partition_split_size = int(sys.argv[2])
    seg_min_size = int(sys.argv[3])
    auto_compact = sys.argv[4].lower() == 'true'
    assert (not auto_compact), 'auto_compact unsupported'
    verb_level = int(sys.argv[5])

    kv_size = 8 + 128
    task = hw.Task(hw.Resource())
    db = dbs.LazyDog(task, partition_split_size, seg_min_size, auto_compact)

    pattern = ops.PatternAppend(times)
    pattern = ops.PatternRand(times, 0, times * 10)
    pattern = ops.PatternRR([
        ops.PatternAppend(times, 1, 0),
        ops.PatternAppend(times, 1, times * 2),
        ops.PatternAppend(times, 1, times * 3),
        ops.PatternAppend(times, 1, times * 4)], times)

    cases.write_then_scan(db, kv_size, task, pattern, times, verb_level)

if __name__ == '__main__':
    lazydog_write_then_scan()
