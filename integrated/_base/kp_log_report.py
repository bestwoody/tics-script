# -*- coding:utf-8 -*-

import sys
import os

def error(msg):
    sys.stderr.write('[kp_log_report.py] ' + msg + '\n')
    sys.exit(1)

def report(std_log_path, err_log_path, color = True):
    lines_limit = 99999
    result_limit = 80

    # TODO: too slow
    std_log = []
    if os.path.exists(std_log_path):
        with open(std_log_path) as file:
            std_log = file.readlines()[-lines_limit:]
    err_log = []
    if os.path.exists(err_log_path):
        with open(err_log_path) as file:
            err_log = file.readlines()[-lines_limit:]

    n = '-'
    e = 'E'
    u = '#'
    if color:
        n = '\033[32m-\033[0m'
        e = '\033[31mE\033[0m'
        u = '\033[33m#\033[0m'

    result = [n for i in range(result_limit)]
    started = False
    started_time = None
    err_log_i = 0

    for sline in std_log:
        if sline.startswith('!RUN '):
            if started:
                result.append(e)
            else:
                if started_time:
                    mark = '!RUN ' + started_time
                    while True:
                        if err_log_i >= len(err_log):
                            break
                        if err_log[err_log_i].startswith(mark):
                            err_log_i += 1
                            break
                        err_log_i += 1
                started = True
            fields = sline.split()
            if len(fields) > 1:
                started_time = fields[1]
        elif sline.startswith('!END '):
            if err_log_i < len(err_log) and not err_log[err_log_i].startswith('!RUN '):
                result.append(u)
            else:
                result.append(n)
            started = False

    return result

if __name__ == '__main__':
    if len(sys.argv) < 3:
        error('usage: <bin> std_log err_log')

    if len(sys.argv) > 3:
        color = (sys.argv[3].lower() == 'color' or (sys.argv[3].lower() == 'true'))
    else:
        color = False
    result = report(sys.argv[1], sys.argv[2], color)[-80:]
    print ''.join(result)
