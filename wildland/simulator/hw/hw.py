import base
from decimal import Decimal

# The factors have been left out:
#   1 dependence between ops
#   2 some of the numbers are not accurate

# TODO: unused yet
class Compressor:
    def __init__(self, cores = 2, compress_bw = 100 * base.MB,
        compress_rate = 0.35, decompress_bw = 400 * base.MB):
        self.reset()

    def reset(self):
        pass

    def on_compress(self, k_num, thread_id = -1):
        return k_num * compress_rate

    def on_decompress(self, compressed_k_num, thread_id = -1):
        return compressed_k_num / decompress_rate

    def elapsed_sec(self, kv_size):
        return Decimal(0.0)

# TODO: unused yet
class Locks:
    def __init__(self, qps_trait = [0, 1e6, 5e5, 2e5, 1e5, 5e4, 2e4]):
        self.qps_trait = qps_trait
        self.reset()

    def reset(self):
        pass

    def on_access(self, lock_name, thread_id = -1):
        pass

    def elapsed_sec(self):
        return Decimal(0.0)

    def load(self, sec):
        return []

# TODO: support multi thread module
class Sorter:
    def __init__(self, cores = 2, sort_bw = 100 * base.MB, sorted_merge_bw = 250 * base.MB):
        self.cores = cores
        self.sort_bw = sort_bw
        self.sorted_merge_bw = sorted_merge_bw
        self.reset()

    def reset(self):
        self._sort_k_num = 0
        self._sorted_merge_k_num = 0

    # TODO: If the k_num is big, it will slower
    def on_sort(self, k_num):
        self._sort_k_num += k_num

    def on_sorted_merge(self, k_num):
        self._sorted_merge_k_num += k_num

    def elapsed_sec(self, kv_size):
        return Decimal(kv_size * self._sort_k_num) / Decimal(self.cores * self.sort_bw) + \
            Decimal(kv_size * self._sorted_merge_k_num) / Decimal(self.cores * self.sorted_merge_bw)

class Processor:
    def __init__(self, compressor, sorter):
        self.compressor = compressor
        self.sorter = sorter

    def reset(self):
        self.compressor.reset()
        self.sorter.reset()

    def elapsed_sec(self, kv_size):
        return self.compressor.elapsed_sec(kv_size) + self.sorter.elapsed_sec(kv_size)

    def load(self, kv_size, sec):
        return [('CPU', sec and (self.elapsed_sec(kv_size) / sec) or 0)]

class Disk:
    # iops with page cache: write 1e6, read 5e6
    def __init__(self, iops_rand = 1e4, iops_seq = 1e6, bw = 400 * base.MB):
        self.iops_rand = iops_rand
        self.iops_seq = iops_seq
        self.bw = bw
        self.reset()

    def reset(self):
        self._seq_write_count = 0
        self._seq_read_count = 0
        self._rand_write_count = 0
        self._rand_read_count = 0
        self._write_k_num = 0
        self._read_k_num = 0

    def on_seq_write(self, k_num):
        self._seq_write_count += 1
        self._write_k_num += k_num

    def on_seq_read(self, k_num):
        self._seq_read_count += 1
        self._read_k_num += k_num

    def on_rand_write(self, k_num):
        self._rand_write_count += 1
        self._write_k_num += k_num

    def on_rand_read(self, k_num):
        self._rand_read_count += 1
        self._read_k_num += k_num

    def total_write_k_num(self):
        return self._write_k_num

    def total_read_k_num(self):
        return self._read_k_num

    def elapsed_sec(self, kv_size):
        return max((self._rand_read_count + self._rand_write_count) / Decimal(self.iops_rand),
            (self._seq_read_count + self._seq_write_count) / Decimal(self.iops_seq),
            (self._read_k_num + self._write_k_num) * (kv_size) / Decimal(self.bw))

    def load(self, kv_size, sec):
        if not sec:
            return []
        rr = Decimal(self._rand_read_count + self._rand_write_count) / Decimal(self.iops_rand) / sec
        sr = Decimal(self._seq_read_count + self._seq_write_count) / Decimal(self.iops_seq) / sec
        return [('IOPS', max(rr, sr)),
            ('IOBW', (self._read_k_num + self._write_k_num) * (kv_size) / (self.bw * sec))]

class Resource:
    def __init__(self, locks = None, compressor = None, sorter = None, disk = None):
        self.locks = locks or Locks()
        self.disk = disk or Disk()
        self.processor = Processor(compressor or Compressor(), sorter or Sorter())

    def reset(self):
        self.locks.reset()
        self.disk.reset()
        self.processor.reset()

    def elapsed_sec(self, kv_size):
        return max(self.locks.elapsed_sec(), self.disk.elapsed_sec(kv_size), self.processor.elapsed_sec(kv_size))

    def load(self, kv_size):
        sec = self.elapsed_sec(kv_size)
        return self.locks.load(sec) + self.disk.load(kv_size, sec) + self.processor.load(kv_size, sec)
