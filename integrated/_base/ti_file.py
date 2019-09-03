# -*- coding:utf-8 -*-

import sys
import os

class Conf:
    def __init__(self):
        self.ti_path = ""
        self.integrated_dir = "-"
        self.conf_templ_dir = "-"
        self.cache_dir = "/tmp/ti"

class Ti:
    def __init__(self):
        self.pds = []
        self.tikvs = []
        self.tidbs = []
        self.tiflashs = []
        self.rngines = []
        self.pd_addr = []

    def dump(self):
        if len(self.pds):
            print 'PDs'
        for it in self.pds:
            print vars(it)
        if len(self.tikvs):
            print 'TiKVs'
        for it in self.tikvs:
            print vars(it)
        if len(self.tidbs):
            print 'TiDBs'
        for it in self.tidbs:
            print vars(it)
        if len(self.tiflashs):
            print 'TiFlashs'
        for it in self.tiflashs:
            print vars(it)
        if len(self.rngines):
            print 'Rngines'
        for it in self.rngines:
            print vars(it)

class Mod:
    def __init__(self, name):
        self.name = name
        self.dir = ""
        self.ports = "+0"
        self.host = ""
        self.pd = ""

class ModRngine:
    def __init__(self):
        self.name = "rngine"
        self.dir = ""
        self.ports = "+0"
        self.host = ""
        self.tiflash = ""
        self.pd = ""

def error(msg):
    sys.stderr.write('[ti_file.py] ' + msg + '\n')
    sys.exit(1)

def parse_kvs(kvs_str, sep = '#'):
    kvs_str = kvs_str.strip()
    kvs = {}
    if len(kvs_str) == 0:
        return kvs
    for it in kvs_str.split(sep):
        kv = it.split('=')
        if len(kv) != 2:
            error('bad prop format: ' + it + ', kvs: ' + kvs_str)
        kvs['{' + kv[0] + '}'] = kv[1]
    return kvs

def parse_mod(obj, line, origin):
    fields = map(lambda x: x.strip(), line.split())
    for field in fields:
        if field.startswith('ports'):
            setattr(obj, 'ports', field[5:].strip())
        elif field.startswith('host'):
            kv = field.split('=')
            if len(kv) != 2 or kv[0].strip() != 'host':
                error('bad host prop: ' + origin)
            setattr(obj, 'host', kv[1].strip())
        elif field.startswith('tiflash'):
            kv = field.split('=')
            if len(kv) != 2 or kv[0].strip() != 'tiflash':
                error('bad tiflash prop: ' + origin)
            setattr(obj, 'tiflash', kv[1].strip())
        elif field.startswith('pd'):
            kv = field.split('=')
            if len(kv) != 2 or kv[0].strip() != 'pd':
                error('bad pd prop: ' + origin)
            setattr(obj, 'pd', kv[1].strip())
        else:
            old_dir = str(getattr(obj, 'dir'))
            if len(old_dir) != 0:
                error('bad line: may be two dir prop: ' + old_dir + ', ' + field + '. line: ' + origin)
            setattr(obj, 'dir', field)
    return obj

def pd(res, line, origin):
    new = parse_mod(Mod('pd'), line, origin)
    i = len(res.pds)
    setattr(new, 'pd_name', 'pd' + str(i))
    res.pds.append(new)
    if len(res.pd_addr) < 3:
        res.pd_addr.append(new.host + ':' + new.ports)

def tikv(res, line, origin):
    res.tikvs.append(parse_mod(Mod('tikv'), line, origin))
def tidb(res, line, origin):
    res.tidbs.append(parse_mod(Mod('tidb'), line, origin))
def tiflash(res, line, origin):
    res.tiflashs.append(parse_mod(Mod('tiflash'), line, origin))
def rngine(res, line, origin):
    res.rngines.append(parse_mod(ModRngine(), line, origin))

mods = {
    'pd' : pd,
    'tikv': tikv,
    'tidb': tidb,
    'tiflash': tiflash,
    'rngine': rngine
}

def parse_file(res, path, kvs):
    lines = open(path).readlines()
    for origin in lines:
        line = origin.strip()
        if len(line) == 0:
            continue
        if line[0] == '#':
            continue

        for k, v in kvs.items():
            line = line.replace(k, v)

        if line.startswith('import'):
            import_path = line[6:].strip()
            if len(import_path) == 0:
                error('bad import line: ' + origin)
            if not (import_path[0] == '/' or import_path[0] == '\\'):
                import_path = os.path.dirname(os.path.abspath(path)) + '/' + import_path
            parse_file(res, import_path, kvs)
        else:
            matched = False
            for mod, func in mods.items():
                if line.startswith(mod):
                    props = line[len(mod):].strip()
                    if props.startswith(':'):
                        func(res, props[1:].strip(), origin)
                        matched = True
                    elif props.startswith('='):
                        pass
                    else:
                        error('bad mod header: ' + origin)
                    break
            if not matched:
                kv = line.split('=')
                if len(kv) != 2:
                    error('bad args line: ' + origin)
                kvs['{' + kv[0].strip() + '}'] = kv[1].strip()

def check_is_valid(res):
    dirs = set()
    for mods in [res.pds, res.tikvs, res.tidbs, res.tiflashs, res.rngines]:
        ports = set()
        for i in range(0, len(mods)):
            mod = mods[i]
            setattr(mod, 'index', i)
            if len(mod.dir) == 0:
                error(mod.name + '[' + str(mod.index) + '].dir can\'t be empty')
            path = mod.host + ':' + mod.dir
            if path in dirs:
                error(mod.name + '[' + str(mod.index) + '].dir duplicated')
            else:
                dirs.add(path)
            if len(mod.host) != 0 and mod.dir[0] != '/':
                error('relative path can\'t use for remote deployment of ' + mod.name + '[' + str(mod.index) + ']: ' + mod.dir)
            addr = mod.host + ':' + mod.ports
            if addr in ports:
                error(mod.name + '[' + str(mod.index) + '].ports duplicated')
            else:
                ports.add(addr)

def print_sh_header(conf, kvs):
    print '#!/bin/bash'
    print ''
    print '# .ti rendered args: ' + str(kvs)
    print ''
    print '# Setup base env (export functions)'
    print 'source "%s/_env.sh"' % conf.integrated_dir
    print 'auto_error_handle'
    print ''
    print 'id="`print_ip_or_host`:%s"' % conf.ti_path

def print_sep():
    print ''
    print '#---------------------------------------------------------'
    print ''

def print_mod_header(mod):
    print_sep()
    host = mod.host
    if len(mod.host) != 0:
        host = '[' + mod.host + '] '
    print 'echo "%s=> %s #%d (%s)"' % (host, mod.name, mod.index, mod.dir)
    print ''

def print_cp_bin(mod, conf):
    line = 'cp_bin_to_dir "%s" "%s" "%s/bin.paths" "%s/bin.urls" "%s/master/bins"'
    print line % (mod.name, mod.dir, conf.conf_templ_dir, conf.conf_templ_dir, conf.cache_dir)
    print ''
    if mod.name == "pd":
        print line % ("ctl-for-pd", mod.dir, conf.conf_templ_dir, conf.conf_templ_dir, conf.cache_dir)
        print ''
    if mod.name == "tikv":
        print line % ("ctl-for-tikv", mod.dir, conf.conf_templ_dir, conf.conf_templ_dir, conf.cache_dir)
        print ''

def print_ssh_prepare(mod, conf, env_dir):
    prepare = 'ssh_prepare_run "%s" ' + mod.name + ' "%s" "%s" "%s" "%s"'
    print prepare % (mod.host, mod.dir, conf.conf_templ_dir, conf.cache_dir, env_dir)

def render_pds(res, conf, hosts, indexes):
    pds = res.pds
    if len(pds) == 0:
        return
    cluster = []
    for i in range(0, len(pds)):
        if i >= 3:
            break
        pd = pds[i]
        addr = pd.host + ':' + pd.ports
        cluster.append(pd.pd_name + '=http://' + addr)
    if len(pds) <= 1 and pds[0].host == '':
        cluster = ''
    else:
        cluster = ','.join(cluster)

    for i in range(0, len(pds)):
        pd = pds[i]
        if len(hosts) != 0 and (pd.host not in hosts):
            continue
        if len(indexes) != 0 and (i not in indexes):
            continue
        print_mod_header(pd)

        if len(pd.host) == 0:
            ssh = ''
            conf_templ_dir = conf.conf_templ_dir
            print_cp_bin(pd, conf)
        else:
            env_dir = conf.cache_dir + '/worker/integrated'
            ssh = 'call_remote_func "%s" "%s" ' % (pd.host, env_dir)
            conf_templ_dir = env_dir + '/conf'
            print_ssh_prepare(pd, conf, env_dir)

        print '# pd_run dir conf_templ_dir ports_delta advertise_host pd_name initial_cluster cluster_id'
        print ssh + 'pd_run "%s" \\' % pd.dir
        print '\t"%s" \\' % conf_templ_dir
        print '\t"%s" "%s" "%s" "%s" \\' % (pd.ports, pd.host, pd.pd_name, cluster)
        print '\t ${id}'

def render_tikvs(res, conf, hosts, indexes):
    for i in range(0, len(res.tikvs)):
        tikv = res.tikvs[i]
        if len(hosts) != 0 and tikv.host not in hosts:
            continue
        if len(indexes) != 0 and (i not in indexes):
            continue
        print_mod_header(tikv)

        if len(tikv.host) == 0:
            ssh = ''
            conf_templ_dir = conf.conf_templ_dir
            print_cp_bin(tikv, conf)
        else:
            env_dir = conf.cache_dir + '/worker/integrated'
            ssh = 'call_remote_func "%s" "%s" ' % (tikv.host, env_dir)
            conf_templ_dir = env_dir + '/conf'
            print_ssh_prepare(tikv, conf, env_dir)

        print '# tikv_run dir conf_templ_dir pd_addr advertise_host ports_delta cluster_id'
        print ssh + 'tikv_run "%s" \\' % tikv.dir
        print '\t"%s" \\' % conf_templ_dir
        pd_addr = tikv.pd or ','.join(res.pd_addr)
        print '\t"%s" "%s" "%s" \\' % (pd_addr, tikv.host, tikv.ports)
        print '\t ${id}'

def render_tidbs(res, conf, hosts, indexes):
    for i in range(0, len(res.tidbs)):
        tidb = res.tidbs[i]
        if len(hosts) != 0 and tidb.host not in hosts:
            continue
        if len(indexes) != 0 and (i not in indexes):
            continue
        print_mod_header(tidb)

        if len(tidb.host) == 0:
            ssh = ''
            conf_templ_dir = conf.conf_templ_dir
            print_cp_bin(tidb, conf)
        else:
            env_dir = conf.cache_dir + '/worker/integrated'
            ssh = 'call_remote_func "%s" "%s" ' % (tidb.host, env_dir)
            conf_templ_dir = env_dir + '/conf'
            print_ssh_prepare(tidb, conf, env_dir)

        print '# tidb_run dir conf_templ_dir pd_addr advertise_host ports_delta cluster_id'
        print ssh + 'tidb_run "%s" \\' % tidb.dir
        print '\t"%s" \\' % conf_templ_dir
        pd_addr = tidb.pd or ','.join(res.pd_addr)
        print '\t"%s" "%s" "%s" \\' % (pd_addr, tidb.host, tidb.ports)
        print '\t ${id}'

def render_tiflashs(res, conf, hosts, indexes):
    if len(res.tiflashs) == 0:
        return

    if len(res.tidbs) != 0:
        print_sep()
        print '# Wait for tidb to ready'
        for tidb in res.tidbs:
            if len(tidb.host) == 0:
                print 'wait_for_tidb "%s"' % tidb.dir
            else:
                print 'wait_for_tidb_by_host "%s" "%s" 180 %s' % (tidb.host, tidb.ports, conf.integrated_dir + '/conf/default.ports')

    if len(res.pds) != 0:
        print_sep()
        print '# Wait for pd to ready'
        for pd in res.pds:
            if len(pd.host) == 0:
                print 'wait_for_pd_local "%s"' % pd.dir
            else:
                bins_dir = conf.cache_dir + '/master/bins'
                print 'wait_for_pd_by_host "%s" "%s" 180 %s %s' % (pd.host, pd.ports, bins_dir, conf.integrated_dir + '/conf/default.ports')

    for i in range(0, len(res.tiflashs)):
        tiflash = res.tiflashs[i]
        if len(hosts) != 0 and tiflash.host not in hosts:
            continue
        if len(indexes) != 0 and (i not in indexes):
            continue
        print_mod_header(tiflash)

        def print_run_cmd(ssh, conf_templ_dir):
            print '# tiflash_run dir conf_templ_dir daemon_mode pd_addr ports_delta listen_host cluster_id'
            print (ssh + 'tiflash_run "%s" \\') % tiflash.dir
            print '\t"%s" \\' % conf_templ_dir
            pd_addr = tiflash.pd and ';'.join(tiflash.pd.split(',')) or ';'.join(res.pd_addr)
            print '\t"true" "%s" "%s" "%s" \\' % (pd_addr, tiflash.ports, tiflash.host)
            print '\t ${id}'

        if len(tiflash.host) == 0:
            print_cp_bin(tiflash, conf)
            print_run_cmd('', conf.conf_templ_dir)
        else:
            env_dir = conf.cache_dir + '/worker/integrated'
            print_ssh_prepare(tiflash, conf, env_dir)
            print_run_cmd('call_remote_func "%s" "%s" ' % (tiflash.host, env_dir), env_dir + '/conf')

def render_rngines(res, conf, hosts, indexes):
    for i in range(0, len(res.rngines)):
        rngine = res.rngines[i]
        if len(hosts) != 0 and rngine.host not in hosts:
            continue
        if len(indexes) != 0 and (i not in indexes):
            continue
        print_mod_header(rngine)

        if len(rngine.host) == 0:
            ssh = ''
            conf_templ_dir = conf.conf_templ_dir
            print_cp_bin(rngine, conf)
        else:
            env_dir = conf.cache_dir + '/worker/integrated'
            ssh = 'call_remote_func "%s" "%s" ' % (rngine.host, env_dir)
            conf_templ_dir = env_dir + '/conf'
            print_ssh_prepare(rngine, conf, env_dir)

        tiflash_host, tiflash_dir = '', ''
        tiflash_addr = map(lambda x: x.strip(), rngine.tiflash.split(':'))
        if len(tiflash_addr) == 2:
            tiflash_host = tiflash_addr[0]
            tiflash_dir = tiflash_addr[1]
        elif len(tiflash_addr) == 1:
            tiflash_dir = tiflash_addr[0]
        else:
            error('bad tiflash address in rngine: ' + rngine.tiflash)
        if len(tiflash_host) == 0:
            tiflash_host = rngine.host

        print '# rngine_run dir conf_templ_dir pd_addr tiflash_addr advertise_host ports_delta cluster_id'
        if len(tiflash_host) == 0:
            print 'tiflash_addr="`get_tiflash_addr_from_dir %s`"' % tiflash_dir
        else:
            print '# ' + str(tiflash_addr)
            print '# ' + tiflash_host
            print '# ' + tiflash_dir
            get_addr_cmd = 'tiflash_addr="`call_remote_func_raw "%s" "%s" get_tiflash_addr_from_dir %s`"'
            env_dir = conf.cache_dir + '/worker/integrated'
            print get_addr_cmd % (tiflash_host, env_dir, tiflash_dir)
        print ssh + 'rngine_run "%s" \\' % rngine.dir
        print '\t"%s" \\' % conf_templ_dir
        pd_addr = rngine.pd or ','.join(res.pd_addr)
        print '\t"%s" "${tiflash_addr}" "%s" "%s" \\' % (pd_addr, rngine.host, rngine.ports)
        print '\t ${id}'

def render(res, conf, kvs, mod_names, hosts, indexes):
    print_sh_header(conf, kvs)
    if len(mod_names) == 0 or ('pd' in mod_names):
        render_pds(res, conf, hosts, indexes)
    if len(mod_names) == 0 or ('tikv' in mod_names):
        render_tikvs(res, conf, hosts, indexes)
    if len(mod_names) == 0 or ('tidb' in mod_names):
        render_tidbs(res, conf, hosts, indexes)
    if len(mod_names) == 0 or ('tiflash' in mod_names):
        render_tiflashs(res, conf, hosts, indexes)
    if len(mod_names) == 0 or ('rngine' in mod_names):
        render_rngines(res, conf, hosts, indexes)

def get_mods(res, mod_names, hosts, indexes):
    confs = {
        'pd' : 'pd.toml',
        'tikv': 'tikv.toml',
        'tidb': 'tidb.toml',
        'tiflash': 'conf/config.xml',
        'rngine': 'rngine.toml'
    }

    def output(mod_array):
        for i in range(0, len(mod_array)):
            mod = mod_array[i]
            if len(indexes) != 0 and (i not in indexes):
                continue
            if len(hosts) == 0 or (mod.host in hosts):
                if len(mod_names) == 0 or (mod.name in mod_names):
                    print '\t'.join([str(i), mod.name, mod.dir, confs[mod.name], mod.host])

    output(res.pds)
    output(res.tikvs)
    output(res.tidbs)
    output(res.tiflashs)
    output(res.rngines)

def get_hosts(res, mod_names, hosts, indexes):
    host_infos = set()
    def output(mod_array):
        for i in range(0, len(mod_array)):
            mod = mod_array[i]
            if len(mod.host) == 0:
                continue
            if len(indexes) != 0 and (i not in indexes):
                continue
            if len(hosts) == 0 or (mod.host in hosts):
                if len(mod_names) == 0 or (mod.name in mod_names):
                    if len(mod.host) != 0 and (mod.host not in host_infos):
                        host_infos.add(mod.host)

    output(res.pds)
    output(res.tikvs)
    output(res.tidbs)
    output(res.tiflashs)
    output(res.rngines)

    for host in host_infos:
        print host

if __name__ == '__main__':
    if len(sys.argv) < 6:
        error('usage: <bin> file cmd(render|mods|hosts) integrated_dir conf_templ_dir cache_dir [mod_nams] [hosts] [indexes] [args_str(k=v#k=v#..)]')

    cmd = sys.argv[1]
    path = sys.argv[2]

    conf = Conf()
    conf.ti_path = path
    conf.integrated_dir = sys.argv[3]
    conf.conf_templ_dir = sys.argv[4]
    conf.cache_dir = sys.argv[5]

    mod_names = set()
    if len(sys.argv) >= 6 and len(sys.argv[6]) != 0:
        mod_names = set(sys.argv[6].split(','))
    for name in mod_names:
        if not mods.has_key(name):
            error(name + 'is not a valid module name')

    hosts = set()
    if len(sys.argv) >= 7 and len(sys.argv[7]) != 0:
        hosts = set(sys.argv[7].split(','))

    indexes = set()
    if len(sys.argv) >= 8 and len(sys.argv[8]) != 0:
        indexes = set(map(lambda x: int(x), sys.argv[8].split(',')))

    kvs_str = ""
    if len(sys.argv) >= 9:
        kvs_str = sys.argv[9]

    res = Ti()
    kvs = parse_kvs(kvs_str)
    parse_file(res, path, kvs)
    check_is_valid(res)

    if cmd == 'render':
        render(res, conf, kvs, mod_names, hosts, indexes)
    elif cmd == 'mods':
        get_mods(res, mod_names, hosts, indexes)
    elif cmd == 'hosts':
        get_hosts(res, mod_names, hosts, indexes)
    else:
        error('unknow cmd: ' + cmd)
