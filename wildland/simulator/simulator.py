import sys

import base
import hw
import dbs
import ops
import cases
import ConfigParser

def load_conf(conf_files):
    conf = ConfigParser.ConfigParser()
    for file in conf_files:
        conf.read(file)
    conf.source = conf_files
    return conf

def display_config(conf, display = base.display, indent = 0):
    display([' ' * indent, 'Config: ', conf.source, '\n'])
    for section in conf.sections():
        display([' ' * (indent + 2), section, '\n'])
        for option in conf.options(section):
            display([' ' * (indent + 4),option, ' = ', conf.get(section, option), '\n'])

def create_lazydog(conf):
    kv_size = conf.getint('db_common', 'kv_size')
    wal_fsync = conf.getboolean('db_common', 'wal_fsync')
    auto_compact = conf.getboolean('db_common', 'auto_compact')
    use_compress = conf.getboolean('db_common', 'use_compress')
    calculate_elapsed = conf.getboolean('bench_common', 'calculate_elapsed')
    db_conf = dbs.CommonConfig(kv_size, auto_compact, wal_fsync, use_compress, calculate_elapsed)

    seg_min = conf.getint('db_partition', 'seg_min')
    seg_max = conf.getint('db_partition', 'seg_max')
    partition_split_size = conf.getint('db_partition', 'partition_split_size')
    wal_group_size = conf.getint('db_partition', 'wal_group_size')

    resource = hw.ResourceSwitch(hw.Resource())
    files_cache_quota = conf.getint('db_cache', 'files_cache_bytes')
    files_cache = dbs.FilesCache(resource, files_cache_quota)
    kvs_cache_quota = conf.getint('db_cache', 'kvs_cache_size')
    kvs_cache = dbs.KvsCache(kvs_cache_quota)

    log_level = conf.get('bench_common', 'log_level')

    env = dbs.Env(base.Log(log_level, base.display), resource, db_conf, files_cache, kvs_cache)
    db = dbs.LazyDog(env, seg_min, seg_max, partition_split_size, wal_group_size)
    return db

def get_verb_level(argv, i, conf):
    verb_level = conf.getint('bench_common', 'verb_level')
    if len(argv) > i and len(argv[i]) > 0:
        verb_level = int(argv[i])
    return verb_level

def lazydog_write_then_scan(argv):
    if len(argv) < 2:
        print 'usage: <bin> pattern config_file [verb-level(0=none, 1=necessary, 2=all)]'
        sys.exit(-1)

    pattern_name = argv[0]
    config_file = argv[1]
    conf = load_conf(['conf/default.conf', config_file])
    verb_level = get_verb_level(argv, 2, conf)

    db = create_lazydog(conf)
    pattern = cases.gen_write_baseline_pattern(pattern_name, conf.getint('bench_common', 'count'))
    assert pattern, 'pattern name: ' + pattern_name + ' not found'

    base.display(db.env.hw.str())
    base.display('---\n')
    display_config(conf)
    base.display('---\n')
    cases.write_then_scan(db, pattern, verb_level)

def lazydog_baseline(argv):
    if len(argv) < 1:
        print 'usage: <bin> config-file [verb-level(0=none, 1=necessary, 2=all)]'
        sys.exit(-1)

    config_file = argv[0]
    conf = load_conf(['conf/default.conf', config_file])
    verb_level = get_verb_level(argv, 1, conf)

    db = create_lazydog(conf)
    base.display(db.env.hw.str())
    base.display('---\n')
    display_config(conf)

    pattern_names = [
        'append',
        'reversed_append',
        'uniform',
        'multi_append',
        'hotzone',
        'hotkeys']

    for pattern_name in pattern_names:
        db = create_lazydog(conf)
        base.display('---\n')
        base.display('pattern: ' + pattern_name + '\n')
        pattern = cases.gen_write_baseline_pattern(pattern_name, conf.getint('bench_common', 'count'))
        assert pattern, 'pattern name: ' + pattern_name + ' not found'
        cases.write_then_scan(db, pattern, verb_level)

def lazydog_oltp(argv):
    if len(argv) < 2:
        print 'usage: <bin> pattern config-file [verb-level(0=none, 1=necessary, 2=all)]'
        sys.exit(-1)

    pattern_name = argv[0]
    config_file = argv[1]
    conf = load_conf(['conf/default.conf', config_file])
    verb_level = get_verb_level(argv, 2, conf)

    db = create_lazydog(conf)
    pattern = cases.gen_oltp_baseline_pattern(pattern_name, conf.getint('bench_common', 'count'))
    assert pattern, 'pattern name: ' + pattern_name + ' not found'

    base.display(db.env.hw.str())
    base.display('---\n')
    display_config(conf)
    base.display('---\n')
    cases.run_mix_ops(db, pattern, verb_level, 'OLTP')

def lazydog_olap(argv):
    if len(argv) < 2:
        print 'usage: <bin> pattern config-file [verb-level(0=none, 1=necessary, 2=all)]'
        sys.exit(-1)

    pattern_name = argv[0]
    config_file = argv[1]
    conf = load_conf(['conf/default.conf', config_file])
    verb_level = get_verb_level(argv, 2, conf)

    db = create_lazydog(conf)

    base.display(db.env.hw.str())
    base.display('---\n')
    display_config(conf)
    base.display('---\n')

    count = conf.getint('bench_common', 'count')
    write_pattern, scan_pattern = cases.gen_olap_baseline_pattern(pattern_name, count)
    assert write_pattern and scan_pattern, 'pattern name: ' + pattern_name + ' not found'
    cases.write(db, write_pattern)
    base.display('---\n')
    cases.run_mix_ops(db, scan_pattern, verb_level, 'OLAP')

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'usage: <bin> module args'
        sys.exit(-1)

    modules = {
        'write_then_scan': lazydog_write_then_scan,
        'baseline': lazydog_baseline,
        'oltp': lazydog_oltp,
        'olap': lazydog_olap,
    }

    name = sys.argv[1]
    if name not in modules:
        print 'error:', name, 'not in', modules.keys()
        sys.exit(-1)

    modules[name](sys.argv[2:])
