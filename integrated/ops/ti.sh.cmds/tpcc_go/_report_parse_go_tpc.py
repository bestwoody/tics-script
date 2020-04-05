# -*- coding:utf-8 -*-

import sys

def run():
    res = {}
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

        if (sec + 10) / 600 - sec / 600 != 1:
            continue
        minu = str((sec + 10) / 60) + 'm'
        if res.has_key(minu):
            continue
        res[minu] = tpm
        print(minu + ' ' + str(tpm))

if __name__ == '__main__':
    run()
