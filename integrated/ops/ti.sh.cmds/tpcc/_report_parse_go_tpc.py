# -*- coding:utf-8 -*-

import sys

def parse():
    res = {}
    lines = []
    while True:
        line = sys.stdin.readline()
        if not line:
            break
        if len(line) == 0:
            continue
        line = line[:-1]
        if line.find(' [SUM] ') < 0:
            continue
        if line.find(' NEW_ORDER ') < 0:
            continue
        i = line.find(' TPM: ')
        j = line.find(', Sum(ms)')
        if i < 0 or j < 0:
            continue
        tpm = int(float(line[i+6:j]))
        i = line.find('Takes(s): ')
        j = line.find(', Count: ')
        if i < 0 or j < 0:
            continue
        sec = int(float(line[i+10:j]))
        sum_tick_sec = 100
        if (sec + sum_tick_sec) / 600 - sec / 600 != 1:
            continue
        minu = str((sec + sum_tick_sec) / 60)
        if minu[-1] == '1':
            minu = minu[:-1] + '0'
        minu += 'm'
        if res.has_key(minu):
            continue
        res[minu] = tpm
        lines.append(minu + ' ' + str(tpm))
    return lines

def run():
    lines = parse()
    res = []
    if len(lines) > 0:
        res.append(lines[0])
    if len(lines) > 2:
        i = (len(lines) - 1) / 2
        res.append(lines[i])
    if len(lines) > 1:
        res.append(lines[-1])
    for line in res:
        print(line)

if __name__ == '__main__':
    run()
