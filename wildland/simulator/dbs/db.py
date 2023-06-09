import heapq
import time
import decimal

import base
import hw

class CommonConfig:
    def __init__(self, kv_size, auto_compact, wal_fsync, use_compress, calculate_elapsed):
        self.kv_size = kv_size
        self.auto_compact = auto_compact
        self.wal_fsync = wal_fsync
        self.use_compress = use_compress
        self.calculate_elapsed = calculate_elapsed

class Env:
    def __init__(self, log, hw, config, files_cache, kvs_cache):
        self.log = log
        self.hw = hw
        self.config = config
        self.files_cache = files_cache
        self.kvs_cache = kvs_cache

class DB:
    def __init__(self, env):
        self.env = env
        self._write_k_num = 0

    def write(self, k, thread_id):
        '''return None'''
        self._write_k_num += 1

    def read(self, k, thread_id, indent = 0, show_id = True):
        '''return ([msg], elapsed_src)'''
        return ['(un-impl)', 'read ', k], 0

    def scan_range(self, begin, end, thread_id, indent = 0, show_id = True):
        '''return ([msg], k_num, elapsed_sec)'''
        return ['(un-impl)', 'scan from ', begin, ' to ', end], 0, 0

    def scan_all(self, thread_id, indent = 0, show_id = True, show_detail = True):
        '''return [msg]'''
        return ['(un-impl)', 'scan all']

    def current_write_amplification(self):
        if self._write_k_num == 0:
            return 0.0
        return decimal.Decimal(self.env.hw.summary.disk.total_write_k_num()) / self._write_k_num

    def k_num(self):
        return self._write_k_num

    def set_snapshot(self, name):
        self._ss_name = name
        self._ss_write_k_num = self._write_k_num
        self.env.hw.switch(name, False)

    def write_k_num_from_snapshot(self):
        return self._write_k_num - self._ss_write_k_num

class Seg:
    def __init__(self, min = base.Limit.maxint, max = base.Limit.minint, sorted = base.Sorted.both, size = 0):
        self.min = min
        self.max = max
        self.sorted = sorted
        self.size = size

    def write(self, k, split_min = base.Limit.maxint, split_max = base.Limit.maxint):
        if self.size == 0:
            self.min = k
            self.max = k
        else:
            if self.sorted == base.Sorted.both:
                if k > self.max:
                    if self.size >= split_max:
                        return False
                    self.max = k
                    self.sorted = base.Sorted.asc
                elif k < self.min:
                    if self.size >= split_max:
                        return False
                    self.min = k
                    self.sorted = base.Sorted.desc
            elif self.sorted == base.Sorted.none:
                if self.size >= split_min:
                    return False
                if k > self.max:
                    self.max = k
                elif k < self.min:
                    self.min = k
            elif self.sorted == base.Sorted.asc:
                if k < self.max:
                    if self.size >= split_min:
                        return False
                    self.sorted = base.Sorted.none
                else:
                    if self.size >= split_max:
                        return False
                    self.max = k
            elif self.sorted == base.Sorted.desc:
                if k > self.min:
                    if self.size >= split_min:
                        return False
                    self.sorted = base.Sorted.none
                else:
                    if self.size >= split_max:
                        return False
                    self.min = k
        self.size += 1
        return True

    def str(self, ref_num, show_id = False):
        id = lambda obj, name: show_id and name + base.obj_id(obj) or name
        size = isinstance(self.size, int) and str(self.size) or ('%.1f' % self.size)
        size = (ref_num > 1) and '(' + size + '/' + str(ref_num) + ')' or '=' + size
        msg = [
            '{', id(self, 'seg:'), ' size', size,
            ' [', base.i2s(self.min), ', ', base.i2s(self.max), '] ',
            base.Sorted.str(self.sorted), '}']
        return msg

class Dist:
    def __init__(self, seg_min, seg_max, info = None, segs = None):
        self._segs = segs or [Seg()]
        self._curr = self._segs[-1]
        self.info = info or Seg()
        self.seg_min = seg_min
        self.seg_max = seg_max

    def write(self, k):
        written = self._curr.write(k, self.seg_min, self.seg_max)
        if not written:
            self._segs.append(Seg())
            self._curr = self._segs[-1]
            written = self._curr.write(k, self.seg_min, self.seg_max)
        assert(written)
        written = self.info.write(k)
        assert(written)

    def str(self, ref_num, show_id = True):
        id = lambda obj, name: show_id and name + base.obj_id(obj) or name
        msg = [id(self, 'dist'), ' ', len(self._segs), ' ', base.Sorted.str(self.info.sorted),' ']
        for seg in self._segs:
            msg += seg.str(ref_num, show_id)
        return msg

class Block:
    def __init__(self, dist):
        self._dist = dist
        self._partitions = []
        self._detacheds = []

    def info(self):
        return self._dist.info

    def size(self, partition):
        if partition in self._detacheds or partition not in self._partitions:
            return decimal.Decimal(0), 0
        stored = self._dist.info.size
        real = stored
        if len(self._partitions) > 1:
            real = decimal.Decimal(stored) / len(self._partitions)
        return real, stored

    def stored_size(self):
        return self._dist.info.size

    def add_ref(self, partition):
        if partition in self._partitions:
            return
        assert partition not in self._detacheds
        self._partitions.append(partition)

    def rm_ref(self, partition):
        if partition in self._detacheds or partition not in self._partitions:
            return
        self._detacheds.append(partition)

    def no_ref(self):
        return len(self._partitions) - len(self._detacheds) == 0

    def refs(self, include_detached):
        if include_detached:
            return set(self._partitions)
        return set(self._partitions).difference(set(self._detacheds))

    def read(self, hw, partition, compressed):
        size, _ = self.size(partition)
        read_size = size
        if compressed:
            read_size = hw.processor.compressor.on_decompress(size)
        hw.disk.on_seq_read(read_size)
        return size

    def str(self, indent = 0, show_id = True):
        id = lambda obj, name: show_id and name + base.obj_id(obj) or name
        msg = [' ' * indent, 'block', id(self, ''), '\n']
        msg += [' ' * (indent + 2), 'ref partitions: ', len(self._partitions), '-', len(self._detacheds)]
        for partition in self._partitions:
            msg += [' ', (partition in self._detacheds and '-' or ''), id(partition, '')]
        msg += ['\n', ' ' * (indent + 2)] + self._dist.str(len(self._partitions), show_id) + ['\n']
        return msg

class File:
    def __init__(self, compressed):
        self._blocks = []
        self._compressed = compressed

    def block_infos(self):
        return map(lambda x: x.info(), self._blocks)

    def size(self, partition):
        real, stored = 0, 0
        for block in self._blocks:
            r, s = block.size(partition)
            real += r
            stored += s
        return real, stored

    def stored_size(self):
        return sum(map(lambda b: b.stored_size(), self._blocks))

    def min(self):
        return min(map(lambda block: block.info().min, self._blocks))

    def max(self):
        return max(map(lambda block: block.info().max, self._blocks))

    def add(self, block):
        self._blocks.append(block)

    def add_ref(self, partition):
        for block in self._blocks:
            block.add_ref(partition)

    def rm_ref(self, partition):
        for block in self._blocks:
            block.rm_ref(partition)

    def no_ref(self):
        for block in self._blocks:
            if not block.no_ref():
                return False
        return True

    def ref_num(self, include_detached):
        partitions = set()
        for block in self._blocks:
            partitions.union(block.refs(include_detached))
        return len(partitions)

    def is_all_blocks_sorted(self):
        for block in  self._blocks:
            if block.info().sorted == base.Sorted.none:
                return False
        return True

    def read(self, hw, partition):
        size = 0
        for block in self._blocks:
            size += block.read(hw, partition, self._compressed)
        return size

    def str(self, indent = 0, show_id = True):
        id = lambda obj, name: show_id and name + base.obj_id(obj) or name
        msg = [' ' * indent, 'file', id(self, ''), ' ', len(self._blocks), ' blocks',
            self._compressed and ' compressed' or '', '\n']
        for block in self._blocks:
            msg += block.str(indent + 2, show_id)
        return msg

def size_in_files(files, partition):
    real, stored = 0, 0
    for file in files:
        r, s = file.size(partition)
        real += r
        stored += s
    return real, stored

class WriteCache:
    def __init__(self, seg_min, seg_max):
        self._dist = Dist(seg_min, seg_max)

    def info(self):
        return self._dist.info

    def size(self):
        return self._dist.info.size

    def min(self):
        return self._dist.info.min

    def max(self):
        return self._dist.info.max

    def write(self, k):
        self._dist.write(k)

    def persist(self, partition, file = None):
        if self.size() <= 0:
            return file
        file = file or File()
        block = Block(self._dist)
        block.add_ref(partition)
        file.add(block)
        partition.add_file(file)
        return file

    def str(self, show_id = True, show_detail = True):
        id = lambda obj, name: show_id and name + base.obj_id(obj) or name
        res = [id(self, 'cache:'), ' size=', self.size(), ' ']
        if show_detail:
            res += self._dist.str(1, show_id)
        return res

class FilesCache:
    def __init__(self, hw, mem_quota_bytes = -1):
        self.hw = hw
        self._mem_quota_bytes = mem_quota_bytes
        self._mem_used = 0
        self._files = set()
        self._file_list = []

    def add(self, file):
        if self._mem_quota_bytes == 0:
            return
        if file in self._files:
            self.pop(file)
        size = file.stored_size()
        self._files.add(file)
        heapq.heappush(self._file_list, (file.ref_num(True), time.time(), size, file))
        self.hw.memory.on_alloc(size)
        self._mem_used += size

        while self._mem_quota_bytes >= 0 and self._mem_quota_bytes < self._mem_used:
            ref_num, created_time, size, file = heapq.heappop(self._file_list)
            self._files.remove(file)
            self.hw.memory.on_free(size)
            self._mem_used -= size

    def pop(self, file):
        for i in range(0, len(self._file_list)):
            ref_num, created_time, size, file_in_list = self._file_list[i]
            if file != file_in_list:
                continue
            self._files.remove(file)
            self._file_list.pop(i)
            self.hw.memory.on_free(size)
            self._mem_used -= size
            break

    def cached(self, file):
        return file in self._files

    def mem_used(self):
        return self._mem_used

    def str(self):
        return map(lambda x: base.obj_id(x), list(self._files))

class KvsCache:
    def __init__(self, quota = -1):
        self._quota = quota
        self._kvs = set()
        self._kv_list = []
        self._hit_count = 0
        self._total_count = 0

    def pop(self, k, hw):
        if self._quota == 0:
            return
        if not k in self._kvs:
            return
        self._kvs.remove(k)
        hw.memory.on_free(1)

    def access(self, k, hw):
        if self._quota == 0:
            return False
        cached = k in self._kvs
        if not cached:
            self._kvs.add(k)
            hw.memory.on_alloc(1)
        else:
            self._hit_count += 1
        heapq.heappush(self._kv_list, (time.time(), k))
        if self._quota > 0 and len(self._kvs) > self._quota:
            while True:
                created_time, k = heapq.heappop(self._kv_list)
                if k in self._kvs:
                    self._kvs.remove(k)
                    hw.memory.on_free(1)
                    break
        self._total_count += 1
        return cached

    def hit_total(self):
        return self._hit_count, self._total_count

    def size(self):
        return len(self._kvs)

    def str(self, kv_size, db_size, indent = 0):
        hit, total = self.hit_total()
        if total <= 0:
            return []
        return [' ' * indent, 'KvsCache hit rate: ', hit, '/', total, ' = %.1f%%' % (float(hit) * 100 / total),
            ', size: ', base.b2s(self.size() * kv_size), ', ', self.size(), ' kv, ',
            '%.0f%% DB Size' % (float(self.size()) * 100 / db_size), '\n']
