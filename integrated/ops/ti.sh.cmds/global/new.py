# -*- coding:utf-8 -*-

import sys
import random

def error(msg):
    sys.stderr.write('[cmd new] ' + msg + '\n')
    sys.exit(1)

def parse(mods, origin):
    names = mods.keys()
    names += ['delta', 'dir']

    argv = []
    for arg in origin:
        arg = arg.strip()
        arg = arg.strip('-')
        if arg.find('=') > 0:
            field = arg.split('=')
            if len(field) != 2:
                error('unknown arg: \'' + arg + '\', not in ' + str(names))
            else:
                argv += field
        else:
            argv.append(arg)

    argv = filter(lambda x: x, argv)

    last_name = None
    for arg in argv:
        if last_name == 'dir' or last_name == 'nodes':
            mods[last_name] = arg
            last_name = None
            continue
        if not arg.strip('-').strip('+').isdigit():
            if arg not in names:
                error('\'' + arg + '\' not in ' + str(names))
            else:
                last_name = arg
        else:
            if not last_name:
                error('\'' + arg + '\': unknown what for')
            mods[last_name] = int(arg)
            last_name = None

def render(mods):
    hosts = []
    nodes = mods['nodes']
    custom_hosts = False
    if nodes.isdigit():
        nodes = int(nodes)
        for i in range(0, nodes):
            hosts.append('TODO')
    else:
        custom_hosts = True
        hosts = nodes.split(',')
        nodes = len(hosts)
    if custom_hosts or nodes > 1:
        for i in range(0, nodes):
            print 'h' + str(i) + '=' + hosts[i]
        print

    if mods.has_key('dir'):
        dir = str(mods['dir'])
    else:
        if nodes != 1:
            dir = '/tmp/nodes'
        else:
            dir = 'nodes'
        n = random.randint(1000, 9999)
        dir = dir + '/' + str(n)

    print 'dir=' + dir
    print

    if mods.has_key('delta'):
        print "delta=" + str(mods['delta'])
    else:
        print "delta=0"

    def out(name):
        mod = mods[name]
        if mod == 0:
            return
        print
        xhead = name + ': {dir}/' + name
        xtail = ' ports+{delta}'
        for i in range(0, mod):
            head = xhead
            tail = xtail
            if mod != 1:
                head += str(i)
                tail += '+' + str(i * 2)
            if name == 'rngine':
                head += ' tiflash={dir}/tiflash'
                if mod != 1:
                    head += str(i)
            if name == 'spark_w':
                head += ' cores=1 mem=1G'
            if custom_hosts or nodes > 1:
                tail += ' host={h' + str(i % nodes) + '}'
            print head + tail

    out('pd')
    out('tikv')
    out('tidb')
    out('tiflash')
    out('rngine')
    out('spark_m')
    out('spark_w')

def new(argv):
    mods = {
        'nodes': '1',
        'pd': 1,
        'tikv': 1,
        'tidb': 1,
        'tiflash': 1,
        'spark': 0,
    }

    # legacy compatibility
    not_num = filter(lambda x: not x.isdigit(), argv)
    if len(not_num) == 0 and len(argv) <= 2:
        if len(argv) >= 1:
            mods['tiflash'] = int(argv[0])
        if len(argv) >= 2:
            mods['tikv'] = int(argv[1])
    else:
        parse(mods, argv)

    mods['rngine'] = mods['tiflash']

    if mods['spark'] != 0:
        mods['spark_m'] = 1
        mods['spark_w'] = mods['spark']
    else:
        mods['spark_m'] = 0
        mods['spark_w'] = 0
    del mods['spark']

    if mods['nodes'] == '1':
        mods['nodes'] = '127.0.0.1'

    render(mods)

if __name__ == '__main__':
    new(sys.argv[1:])
