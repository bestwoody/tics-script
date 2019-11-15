# -*- coding:utf-8 -*-

import sys

def unfold(sep):
    total = []
    res = []

    while True:
        line = sys.stdin.readline()
        if not line:
            break
        if len(line) == 0:
            continue
        line = line[:-1].strip()

        i = line.find(sep)
        if i < 0:
            res.append('"' + line.strip() + '"')
            continue

        fields = line.split(sep)
        fields = map(lambda x: x.strip(), fields)

        if len(fields) == 0:
            total.append(res)
            res = []
            continue

        if len(fields[0]) > 0:
            res.append('"' + fields[0] + '"')
        total.append(res)
        if len(fields[-1]) > 0:
            res = ['"' + fields[-1] + '"']
        else:
            res = []
        fields = fields[1:-1]

        fields = filter(lambda x: len(x) > 0, fields)
        fields = map(lambda x: '"' + x + '"', fields)
        for field in fields:
            if len(field) > 0:
                total.append([field])

    total.append(res)
    res = None
    total = filter(lambda x: len(x) > 0, total)

    for cmd_args in total:
        cmd = cmd_args[0]
        assert cmd[0] == '"', cmd
        assert cmd[-1] == '"', cmd
        cmd = cmd[1:-1]
        cmd_args = cmd_args[1:]
        if len(cmd_args) > 0:
            print cmd + '\t' + ' '.join(cmd_args)
        else:
            cmd_args = cmd.split()
            cmd = cmd_args[0]
            cmd_args = cmd_args[1:]
            print cmd + '\t' + ' '.join(cmd_args)

if __name__ == '__main__':
    unfold(':')
