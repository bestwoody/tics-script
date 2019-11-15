# -*- coding:utf-8 -*-

import sys

# TODO: this func is the same as in the 'unfold_cmd_chain.py'
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

    return total

def render(total, sep):
    sep = '"' + sep + '"'
    for cmd in total:
        once = []
        loop = []
        for i in range(0, len(cmd)):
            field = cmd[i]
            if field != sep:
                once.append(field)
            else:
                loop = cmd[i + 1:]
                break
        has_loop = (len(loop) != 0) and 'has_loop' or 'no_loop'
        print ' '.join(once) + '\t' + has_loop + '\t' + ' '.join(loop)

if __name__ == '__main__':
    render(unfold('GO:'), 'LOOP:')
