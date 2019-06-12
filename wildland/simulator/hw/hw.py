import decimal
import copy

import base

class Compressor:
    def __init__(self, cores = 2, compress_bw = 100 * base.MB,
        compress_rate = 0.35, decompress_bw = 300 * base.MB, name = 'lz4'):

        self.name = name
        self.cores = cores
        self.compress_bw = compress_bw
        self.compress_rate = decimal.Decimal(compress_rate)
        self.decompress_bw = decompress_bw
        self.reset()

    def reset(self):
        self._compressed_k_num = 0
        self._decompressed_k_num = 0
        self.set_snapshot()

    def set_snapshot(self):
        self._ss_compressed_k_num = self._compressed_k_num
        self._ss_decompressed_k_num = self._decompressed_k_num

    def on_compress(self, k_num):
        self._compressed_k_num += k_num
        return k_num * self.compress_rate

    def on_decompress(self, origin_k_num):
        compressed_k_num = origin_k_num * self.compress_rate
        self._decompressed_k_num += origin_k_num
        return compressed_k_num

    def elapsed_sec(self, kv_size):
        compressed_bytes = decimal.Decimal(kv_size * self._compressed_k_num)
        decompressed_bytes = decimal.Decimal(kv_size * self._decompressed_k_num)
        compressed_sec = compressed_bytes / (self.cores * self.compress_bw)
        decompressed_sec = decompressed_bytes / (self.cores * self.decompress_bw)
        return compressed_sec + decompressed_sec

    def elapsed_sec_from_snapshot(self, kv_size):
        compressed_bytes = decimal.Decimal(kv_size) * (self._compressed_k_num - self._ss_compressed_k_num)
        decompressed_bytes = decimal.Decimal(kv_size) * (self._decompressed_k_num - self._ss_decompressed_k_num)
        compressed_sec = compressed_bytes / (self.cores * self.compress_bw)
        decompressed_sec = decompressed_bytes / (self.cores * self.decompress_bw)
        return compressed_sec + decompressed_sec

    def load(self, kv_size, sec):
        if sec <= 0:
            return []
        compressed_bytes = decimal.Decimal(kv_size * self._compressed_k_num)
        decompressed_bytes = decimal.Decimal(kv_size * self._decompressed_k_num)
        compress_load = compressed_bytes / (self.cores * self.compress_bw) / sec
        decompress_load = decompressed_bytes / (self.cores * self.decompress_bw) / sec
        res = []
        if compressed_bytes > 0:
            res.append(('COMPRESS', '%.1f%% CPU' % (compress_load * 100)))
        if decompressed_bytes > 0:
            res.append(('DECOMPRESS', '%.1f%% CPU' % (decompress_load * 100)))
        return res

    def str(self, indent):
        indent2 = ' ' * (indent + 2)
        return [
            ' ' * indent, 'Compressor perf (per core): ', self.name, '\n',
            indent2, 'Compress: ', base.b2s(self.compress_bw), '/s\n',
            indent2, 'Compress rate: ', '%.1f%%' % (self.compress_rate * 100), '\n',
            indent2, 'Decompress: ', base.b2s(self.decompress_bw), '/s\n']

class Locks:
    def __init__(self, tps_trait = [0, 1e6, 2e5, 1e5, 5e4]):
        self.tps_trait = tps_trait
        self.reset()

    def reset(self):
        self._locks = {}
        self.set_snapshot()

    def set_snapshot(self):
        pass

    def on_access_obj(self, obj, thread_id):
        lock_name = obj.__class__.__name__ + '@' + str(id(obj))
        self.on_access(lock_name, thread_id)

    def on_access(self, lock_name, thread_id):
        if lock_name not in self._locks:
            self._locks[lock_name] = {thread_id: 1}
        else:
            threads = self._locks[lock_name]
            if thread_id not in threads:
                threads[thread_id] = 1
            else:
                threads[thread_id] = threads[thread_id] + 1

    def _get_highest(self, locks):
        def cal_content_num(threads):
            if len(threads) <= 1:
                return len(threads)
            max_count = max(map(lambda item: item[1], threads.items()))
            threshold = max_count / 3.0
            num = 0
            for thread_id, count in threads.iteritems():
                if count > threshold:
                    num += 1
            return num

        def cal_sec(content_num, access_count):
            content = 1
            for tps in self.tps_trait:
                if content_num <= content:
                    if tps <= 0:
                        return decimal.Decimal(0.0), '(max)'
                    return access_count / decimal.Decimal(tps), tps
                content = content * 2
            return access_count / decimal.Decimal(tps), tps

        def get_sec(item):
            lock_name, threads = item
            content_num = cal_content_num(threads)
            access_count = sum(threads.values())
            sec, tps = cal_sec(content_num, access_count)
            return (sec, content_num, access_count, tps, lock_name)

        locks = map(get_sec, locks.items())
        locks.sort()
        return locks[-1]

    def elapsed_sec(self):
        if len(self._locks) == 0:
            return decimal.Decimal(0.0)
        sec, thread_num, access_count, tps, lock_name = self._get_highest(self._locks)
        return sec

    def elapsed_sec_from_snapshot(self):
        return self.elapsed_sec()

    def load(self, sec):
        if len(self._locks) == 0 or sec <= 0:
            return []
        used_sec, thread_num, access_count, tps, lock_name = self._get_highest(self._locks)
        load = '%.1f%%' % (100 * used_sec / sec)
        load += ' (' + str(thread_num) + ' threads, ' + str(access_count) + ' access, ' + str(tps) + ' tps)'
        return [('LOCK(PEAK)', load)]

    def str(self, indent):
        res = [' ' * indent + 'Lock perf', '\n']
        content = 1
        for tps in self.tps_trait:
            res += [' ' * (indent + 2), str(content), ' threads: ', tps and long(tps) or 'max', '/s\n']
            content = content * 2
        return res

class Sorter:
    def __init__(self, cores = 2, sort_bw = 100 * base.MB, sorted_merge_bw = 250 * base.MB):
        self.cores = cores
        self.sort_bw = sort_bw
        self.sorted_merge_bw = sorted_merge_bw
        self.reset()

    def reset(self):
        self._sort_k_num = 0
        self._sorted_merge_k_num = 0
        self.set_snapshot()

    def set_snapshot(self):
        self._ss_sort_k_num = self._sort_k_num
        self._ss_sorted_merge_k_num = self._sorted_merge_k_num

    # TODO: If the k_num is big, it will slower
    def on_sort(self, k_num):
        self._sort_k_num += k_num

    def on_sorted_merge(self, k_num):
        self._sorted_merge_k_num += k_num

    def elapsed_sec(self, kv_size):
        sorted_bytes = decimal.Decimal(kv_size * self._sort_k_num)
        merged_bytes = decimal.Decimal(kv_size * self._sorted_merge_k_num)
        sort_sec = sorted_bytes / (self.cores * self.sort_bw)
        merge_sec = merged_bytes / (self.cores * self.sorted_merge_bw)
        return sort_sec + merge_sec

    def elapsed_sec_from_snapshot(self, kv_size):
        sorted_bytes = decimal.Decimal(kv_size) * (self._sort_k_num - self._ss_sort_k_num)
        merged_bytes = decimal.Decimal(kv_size) * (self._sorted_merge_k_num - self._ss_sorted_merge_k_num)
        sort_sec = sorted_bytes / (self.cores * self.sort_bw)
        merge_sec = merged_bytes / (self.cores * self.sorted_merge_bw)
        return sort_sec + merge_sec

    def load(self, kv_size, sec):
        if sec <= 0:
            return []
        sorted_bytes = decimal.Decimal(kv_size * self._sort_k_num)
        merged_bytes = decimal.Decimal(kv_size * self._sorted_merge_k_num)
        sort_load = sorted_bytes / (self.cores * self.sort_bw) / sec
        merge_load = merged_bytes / (self.cores * self.sorted_merge_bw) / sec
        res = []
        if sorted_bytes > 0:
            res.append(('SORT', '%.1f%% CPU' % (sort_load * 100)))
        if merged_bytes > 0:
            res.append(('MERGE(SORTED)', '%.1f%% CPU' % (merge_load * 100)))
        return res

    def str(self, indent):
        return [
            ' ' * indent, 'Sortor perf (per core)\n',
            ' ' * (indent + 2), 'Sort: ', base.b2s(self.sort_bw), '/s\n',
            ' ' * (indent + 2), 'Merge sorted: ', base.b2s(self.sorted_merge_bw), '/s\n']

class Processor:
    def __init__(self, compressor, sorter):
        self.compressor = compressor
        self.sorter = sorter
        assert self.compressor.cores == self.sorter.cores
        self.cores = self.compressor.cores

    def reset(self):
        self.compressor.reset()
        self.sorter.reset()

    def set_snapshot(self):
        self.compressor.set_snapshot()
        self.sorter.set_snapshot()

    def elapsed_sec(self, kv_size):
        return self.compressor.elapsed_sec(kv_size) + self.sorter.elapsed_sec(kv_size)

    def elapsed_sec_from_snapshot(self, kv_size):
        return self.compressor.elapsed_sec_from_snapshot(kv_size) + self.sorter.elapsed_sec_from_snapshot(kv_size)

    def load(self, kv_size, sec):
        load = sec and (self.elapsed_sec(kv_size) * 100/ sec) or 0
        sub = self.compressor.load(kv_size, sec) + self.sorter.load(kv_size, sec)
        return [('CPU', '%.1f%%' % load)] + map(lambda x: ('  ' + x[0], x[1]), sub)

    def str(self, indent):
        res = [' ' * indent, 'CPU: ', self.cores, ' cores\n']
        res += self.compressor.str(indent + 2) + self.sorter.str(indent + 2)
        return res

class Disk:
    # iops with page cache: write 1e6, read 5e6
    def __init__(self, iops_rand = 1e5, iops_seq = 1e6, iops_fsync = 5e3, bw = 400 * base.MB):
        self.iops_rand = iops_rand
        self.iops_seq = iops_seq
        self.iops_fsync = iops_fsync
        self.bw = bw
        self.reset()

    def reset(self):
        self._seq_write_count = 0
        self._seq_read_count = 0
        self._rand_write_count = 0
        self._rand_read_count = 0
        self._fsync_count = 0
        self._write_k_num = 0
        self._read_k_num = 0
        self.set_snapshot()

    def set_snapshot(self):
        self._ss_seq_write_count = self._seq_write_count
        self._ss_seq_read_count = self._seq_read_count
        self._ss_rand_write_count = self._rand_write_count
        self._ss_rand_read_count = self._rand_read_count
        self._ss_fsync_count = self._fsync_count
        self._ss_write_k_num = self._write_k_num
        self._ss_read_k_num = self._read_k_num

    def on_seq_write(self, k_num, fsync):
        self._seq_write_count += 1
        self._fsync_count += fsync and 1 or 0
        self._write_k_num += k_num

    def on_seq_read(self, k_num):
        self._seq_read_count += 1
        self._read_k_num += k_num

    def on_rand_write(self, k_num, fsync):
        self._rand_write_count += 1
        self._fsync_count += fsync and 1 or 0
        self._write_k_num += k_num

    def on_rand_read(self, k_num):
        self._rand_read_count += 1
        self._read_k_num += k_num

    def total_write_k_num(self):
        return self._write_k_num

    def total_read_k_num(self):
        return self._read_k_num

    def elapsed_sec(self, kv_size):
        return max(
            (self._rand_read_count + self._rand_write_count) / decimal.Decimal(self.iops_rand),
            (self._seq_read_count + self._seq_write_count) / decimal.Decimal(self.iops_seq),
            (self._fsync_count) / decimal.Decimal(self.iops_fsync),
            (self._read_k_num + self._write_k_num) * (kv_size) / decimal.Decimal(self.bw))

    def elapsed_sec_from_snapshot(self, kv_size):
        seq_write_count = self._seq_write_count - self._ss_seq_write_count
        seq_read_count = self._seq_read_count - self._ss_seq_read_count
        rand_write_count = self._rand_write_count - self._ss_rand_write_count
        rand_read_count = self._rand_read_count - self._ss_rand_read_count
        fsync_count = self._fsync_count - self._ss_fsync_count
        write_k_num = self._write_k_num - self._ss_write_k_num
        read_k_num = self._read_k_num - self._ss_read_k_num
        return max(
            (rand_read_count + rand_write_count) / decimal.Decimal(self.iops_rand),
            (seq_read_count + seq_write_count) / decimal.Decimal(self.iops_seq),
            (fsync_count) / decimal.Decimal(self.iops_fsync),
            (read_k_num + write_k_num) * (kv_size) / decimal.Decimal(self.bw))

    def load(self, kv_size, sec):
        res = []
        if not sec:
            return res
        if self._seq_read_count + self._seq_write_count != 0:
            read_ps = decimal.Decimal(self._seq_read_count) / sec
            write_ps = decimal.Decimal(self._seq_write_count) / sec
            total_ps = read_ps + write_ps
            total_util = total_ps * 100 / decimal.Decimal(self.iops_seq)
            res.append(('IOPS(SEQ)', str(int(total_ps)) + '/s, ' + '%.1f%%' % total_util))
            if read_ps > 0:
                read_util = read_ps * 100 / decimal.Decimal(self.iops_seq)
                res.append(('  IOPS(SEQ)(READ)', str(int(read_ps)) + '/s, ' + '%.1f%%' % read_util))
            if write_ps > 0:
                write_util = write_ps * 100 / decimal.Decimal(self.iops_seq)
                res.append(('  IOPS(SEQ)(WRITE)', str(int(write_ps)) + '/s, ' + '%.1f%%' % write_util))
        if self._rand_read_count + self._rand_write_count != 0:
            read_ps = decimal.Decimal(self._rand_read_count) / sec
            write_ps = decimal.Decimal(self._rand_write_count) / sec
            total_ps = read_ps + write_ps
            total_util = total_ps * 100 / decimal.Decimal(self.iops_rand)
            res.append(('IOPS(RAND)', str(int(total_ps)) + '/s, ' + '%.1f%%' % total_util))
            if read_ps > 0:
                read_util = read_ps * 100 / decimal.Decimal(self.iops_rand)
                res.append(('  IOPS(RAND)(READ)', str(int(read_ps)) + '/s, ' + '%.1f%%' % read_util))
            if write_ps > 0:
                write_util = write_ps * 100 / decimal.Decimal(self.iops_rand)
                res.append(('  IOPS(RAND)(WRITE)', str(int(write_ps)) + '/s, ' + '%.1f%%' % write_util))
        if self._fsync_count != 0:
            fsync_ps = decimal.Decimal(self._fsync_count) / sec
            fsync_util = fsync_ps * 100 / decimal.Decimal(self.iops_fsync)
            res.append(('IOPS(FSYNC)', str(int(fsync_ps)) + '/s, ' + '%.1f%%' % fsync_util))
        if self._read_k_num + self._write_k_num != 0:
            read_io_size = self._read_k_num * kv_size
            write_io_size = self._write_k_num * kv_size
            total_io_size = read_io_size + write_io_size
            total_bw_util = '%.1f%%' % (total_io_size * 100 / (self.bw * sec))
            res.append(('IOBW', base.b2s(total_io_size / sec) + '/s, ' + total_bw_util))
            if read_io_size > 0:
                read_bw_util = '%.1f%%' % (read_io_size * 100 / (self.bw * sec))
                res.append(('  IOBW(READ)', base.b2s(read_io_size / sec) + '/s, ' + read_bw_util))
            if write_io_size > 0:
                write_bw_util = '%.1f%%' % (write_io_size * 100 / (self.bw * sec))
                res.append(('  IOBW(WRITE)', base.b2s(write_io_size / sec) + '/s, ' + write_bw_util))
        return res

    def str(self, indent):
        return [
            ' ' * indent, 'Disk\n',
            ' ' * (indent + 2), 'IOPS(SEQ): ', long(self.iops_seq), '/s\n',
            ' ' * (indent + 2), 'IOPS(RAND): ', long(self.iops_rand), '/s\n',
            ' ' * (indent + 2), 'IOPS(FSYNC): ', long(self.iops_fsync), '/s\n',
            ' ' * (indent + 2), 'IOBW: ', base.b2s(self.bw), '/s\n']

class Api:
    def __init__(self):
        self.reset()

    def reset(self):
        self._output_k_num = 0

    def on_output(self, k_num):
        self._output_k_num += k_num

    def total_output_k_num(self):
        return self._output_k_num

    def str(self, indent):
        return []

class Memory:
    def __init__(self):
        self._curr_use_k_num = 0
        self._peak_use_k_num = 0

    def on_alloc(self, k_num = 1):
        self._curr_use_k_num += k_num
        self._peak_use_k_num = max(self._curr_use_k_num, self._peak_use_k_num)

    def on_free(self, k_num):
        self._curr_use_k_num -= k_num

    def load(self, kv_size):
        return [('MEM(PEAK)', base.b2s(long(self._peak_use_k_num * 10) * kv_size / 10))]

    def str(self, indent):
        return []

class Resource:
    def __init__(self, locks = None, compressor = None, sorter = None, disk = None):
        self.locks = locks or Locks()
        self.disk = disk or Disk()
        self.processor = Processor(compressor or Compressor(), sorter or Sorter())
        self.api = Api()
        self.memory = Memory()

    def str(self):
        res = ['Hardware module\n']
        res += self.locks.str(2) + self.processor.str(2) + self.disk.str(2)
        return res

    def reset(self):
        self.locks.reset()
        self.api.reset()
        self.disk.reset()
        self.processor.reset()

    def set_snapshot(self):
        self.locks.set_snapshot()
        self.disk.set_snapshot()
        self.processor.set_snapshot()

    def elapsed_sec(self, kv_size):
        return max(self.locks.elapsed_sec(), self.disk.elapsed_sec(kv_size), self.processor.elapsed_sec(kv_size))

    def elapsed_sec_from_snapshot(self, kv_size):
        return max(self.locks.elapsed_sec_from_snapshot(), self.disk.elapsed_sec_from_snapshot(kv_size),
            self.processor.elapsed_sec_from_snapshot(kv_size))

    def load(self, kv_size):
        sec = self.elapsed_sec(kv_size)
        return self.processor.load(kv_size, sec) + self.memory.load(kv_size) + \
            self.locks.load(sec) + self.disk.load(kv_size, sec)

    def clone(self):
        resource = copy.deepcopy(self)
        resource.reset()
        return resource
