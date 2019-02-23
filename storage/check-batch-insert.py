# -*- coding:utf-8 -*-
import numpy
import subprocess
import sys


def python3_to_bytes(x, signed):
    return int(x).to_bytes(length=8, byteorder='little', signed=signed)


to_bytes = python3_to_bytes


def get_col_types():
    data = sys.stdin.readlines()
    col_types = [col.split('\t')[1] for col in data]
    # print(col_types)
    return col_types


def check_int(x, bytes_id, size, signed):
    assert bytes_id[:size] == to_bytes(x, signed)[:size]


def check_int64(x, bytes_id, _):
    check_int(x, bytes_id, 8, True)


def check_int32(x, bytes_id, _):
    check_int(x, bytes_id, 4, True)


def check_int16(x, bytes_id, _):
    check_int(x, bytes_id, 2, True)


def check_int8(x, bytes_id, _):
    check_int(x, bytes_id, 1, True)


def check_uint64(x, bytes_id, _):
    check_int(x, bytes_id, 8, False)


def check_uint32(x, bytes_id, _):
    check_int(x, bytes_id, 4, False)


def check_uint16(x, bytes_id, _):
    check_int(x, bytes_id, 2, False)


def check_uint8(x, bytes_id, _):
    check_int(x, bytes_id, 1, False)


def check_str(x, _, id_num):
    p = chr(id_num % 70 + ord('0')) * len(x)
    assert x == p.encode()


def check_float32(x, _, id_num):
    assert numpy.float32(float(x)) == numpy.float32(float(id_num) / 1111.1)


def check_float64(x, _, id_num):
    assert float(id_num) / 1111.1 == float(x)


check_func = {
    'Int64': check_int64,
    'Int32': check_int32,
    'Int16': check_int16,
    'Int8': check_int8,
    'UInt64': check_uint64,
    'UInt32': check_uint32,
    'UInt16': check_uint16,
    'UInt8': check_uint8,
    'String': check_str,
    'Float32': check_float32,
    'Float64': check_float64,
}


def main():
    path = '/tmp/batch-insert-data'
    if len(sys.argv) > 1:
        path = sys.argv[1]
    total = 0
    all_ids = set()
    col_types = get_col_types()
    with open(path, 'rb') as f:
        for line in f:
            line = line[:-1]
            if not line:
                continue
            total += 1
            array = line.split(b'\t')
            id_num = int(array[len(col_types)-1])
            bytes_id = to_bytes(id_num, False)
            for idx, col_type in enumerate(col_types):
                if col_type not in check_func:
                    raise Exception("Type %s not exists in schema" % col_type)
                check_func[col_type](array[idx], bytes_id, id_num)
            all_ids.add(id_num)
        f.close()
    assert len(all_ids) == total
    min_id, max_id = min(all_ids), max(all_ids)
    assert total == max_id - min_id + 1

    try:
        assert total == max_id - min_id + 1
    except AssertionError as e:
        print("max_id: %d, min_id: %d" % (max_id, min_id), file=sys.stderr)
        raise e
    print("check ok. total num: %d" % (len(all_ids)))


def run_subprocess(shell_cmd):
    p = subprocess.Popen(shell_cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    while p.poll() is None:
        line = p.stdout.readline()
        line = line.strip()
        if line:
            print('Subprogram output: [{}]'.format(line))
    if p.returncode == 0:
        print('Subprogram success')
    else:
        print('Subprogram failed')


if __name__ == '__main__':
    main()
