import decimal
import base
import db

class Partition:
    def __init__(self, env, seg_min, seg_max, files = []):
        self.env = env
        self.seg_min = seg_min
        self.seg_max = seg_max

        self._files = []
        for file in files:
            self._files.append(file)
            # TODO: can filter some segs by partition[begin, end)
            file.add_ref(self)

        self._write_cache = db.WriteCache(seg_min, seg_max)

    def write(self, k, thread_id):
        self._write_cache.write(k)
        self.env.hw.locks.on_access_obj(self._write_cache, thread_id)
        self.env.hw.disk.on_seq_write(1, self.env.config.wal_fsync)
        self.env.hw.memory.on_alloc(1)

    def size_in_files(self):
        return db.size_in_files(self._files, self)

    def size_in_cache(self):
        return self._write_cache.size()

    def min(self):
        res = map(lambda file: file.min(), self._files)
        res.append(self._write_cache.min())
        return min(res)

    def max(self):
        res = map(lambda file: file.max(), self._files)
        res.append(self._write_cache.max())
        return max(res)

    def empty(self):
        _, stored = self.size_in_files()
        return self._write_cache.size() == 0 and stored == 0

    def files(self):
        return set(self._files)

    def sorted(self):
        # TODO: too slow, use iterator
        segs = []
        for file in self._files:
            segs += file.segs()
        segs += self._write_cache.segs()
        return db.is_sorted(segs)

    def add_file(self, file):
        self._files.append(file)

    def persist_write_cache(self, file = None):
        self._write_cache.persist(self, file)
        stored_size = self._write_cache.size()
        self.env.hw.memory.on_free(stored_size)
        self._write_cache = db.WriteCache(self.seg_min, self.seg_max)

    def _compact(self, begin, end, indent, show_id, show_detail):
        files, include_write_cache = self._select_compact_files(begin, end, indent, show_id, show_detail)
        return self._do_compact(files, include_write_cache, begin, end, indent, show_id, show_detail)

    # TODO: return unsorted files
    def _select_compact_files(self, begin, end, indent, show_id, show_detail):
        '''return ([files], include_write_cache)'''
        return map(lambda x: x, self._files), True

    def _do_compact(self, files, include_write_cache, begin, end, indent = 0, show_id = True, show_detail = True):
        # TODO: form sorted multi-segs
        size, _ = db.size_in_files(files, self)
        if include_write_cache:
            size += self._write_cache.size()

        new_min = self.min()
        new_max = self.max()
        if include_write_cache:
            new_min = min(new_min, self._write_cache.min())
            new_max = max(new_max, self._write_cache.max())
        new_min = max(new_min, begin)
        new_max = min(new_max, end - 1)

        seg = db.Seg(new_min, new_max, base.Sorted.asc, size)

        # TODO: move this to db.py
        dist = db.Dist(self.seg_min, self.seg_max, [seg])
        block = db.Block(dist)
        block.add_ref(self)
        to_file = db.File(self.env.config.use_compress)
        to_file.add(block)
        self.env.files_cache.add(to_file)

        for file in files:
            file.rm_ref(self)
            if file.no_ref():
                self.env.files_cache.pop(file)
            self._files.remove(file)

        self._files.append(to_file)
        if include_write_cache:
            self._write_cache = db.WriteCache(self.seg_min, self.seg_max)
        return size

    # TODO: faster
    def read(self, k, begin, end, indent = 0, show_id = True, show_detail = True):
        allow_compact = self.env.config.auto_compact
        msg, size, compacting = self._scan(begin, end, allow_compact, indent, show_id, show_detail)
        if compacting:
            self._compact(begin, end, 0, show_id, False)
            write_size = size
            if self.env.config.use_compress:
                write_size = self.env.hw.processor.compressor.on_compress(size)
            self.env.hw.disk.on_seq_write(write_size, True)
            rescan_msg, size, compacting = self._scan(begin, end, False, indent, show_id, False)
            assert not compacting
            if show_detail:
                for file in self._files:
                    msg += file.str(indent + 2, show_id)
            msg += rescan_msg
        self.env.hw.api.on_output(1)
        return msg

    def scan_range(self, r_begin, r_end, begin, end, indent = 0, show_id = True, show_detail = True):
        allow_compact = self.env.config.auto_compact
        msg, size, compacting = self._scan(begin, end, allow_compact, indent, show_id, show_detail)
        if compacting:
            self._compact(begin, end, 0, show_id, False)
            write_size = size
            if self.env.config.use_compress:
                write_size = self.env.hw.processor.compressor.on_compress(size)
            self.env.hw.disk.on_seq_write(write_size, True)
            rescan_msg, size, compacting = self._scan(begin, end, False, indent, show_id, False)
            assert not compacting
            if show_detail:
                for file in self._files:
                    msg += file.str(indent + 2, show_id)
            msg += rescan_msg
        p_min = decimal.Decimal(self.min())
        p_max = decimal.Decimal(self.max())
        read_rate = (min(r_end, p_max) - max(r_begin, p_min) + 1) / (p_max - p_min)
        read_size = read_rate * size
        self.env.hw.api.on_output(read_size)
        return msg, read_size

    def scan_all(self, begin, end, indent = 0, show_id = True, show_detail = True):
        allow_compact = self.env.config.auto_compact
        msg, size, compacting = self._scan(begin, end, allow_compact, indent, show_id, show_detail)
        if compacting:
            self._compact(begin, end, 0, show_id, False)
            write_size = size
            if self.env.config.use_compress:
                write_size = self.env.hw.processor.compressor.on_compress(size)
            self.env.hw.disk.on_seq_write(write_size, True)
            rescan_msg, size, compacting = self._scan(begin, end, False, indent, show_id, False)
            assert not compacting
            if show_detail:
                for file in self._files:
                    msg += file.str(indent + 2, show_id)
            msg += rescan_msg
        self.env.hw.api.on_output(size)
        return msg

    def _scan(self, begin, end, allow_compact, indent = 0, show_id = True, show_detail = True):
        id = lambda obj, name: show_id and name + base.obj_id(obj) or name
        msg = []

        real_size, stored_size = self.size_in_files()
        cached_size = self.size_in_cache()

        if show_detail:
            if len(self._files) > 0 or cached_size > 0:
                msg += [' ' * indent, 'stored-min-max: ', base.i2s(self.min()),
                    ', ', base.i2s(self.max()), '\n']
            if self._write_cache.size() > 0:
                msg += [' ' * indent] + self._write_cache.str(show_id, show_detail) + ['\n']

        real_size_disk = 0
        real_size_cache = 0
        read_size_disk = 0
        read_size_cache = 0
        files_in_disk = set()
        files_in_cache = set()
        if len(self._files) > 0:
            if show_detail:
                msg += [' ' * indent, 'disk-size: real=', '%.1f' % real_size,
                    ' kv, stored=', '%.1f' % stored_size, ' kv\n']
                msg += [' ' * indent, 'files: ', len(self._files), '\n']
            for file in self._files:
                if not self.env.files_cache.cached(file):
                    file.read(self.env.hw, self)
                r, s = file.size(self)
                if not self.env.files_cache.cached(file):
                    real_size_disk += r
                    read_size_disk += s
                    files_in_disk.add(file)
                else:
                    real_size_cache += r
                    read_size_cache += s
                    files_in_cache.add(file)
                self.env.files_cache.add(file)
                if show_detail:
                    msg += file.str(indent + 2, show_id)

        streams = (cached_size > 0 and 1 or 0) + len(self._files)
        size = cached_size + real_size

        io_plan = []
        if read_size_disk > 0:
            disk_plan = 'disk(' + '%.1f' % read_size_disk + ' kv, ' + str(len(files_in_disk)) + ' files)'
            if real_size_disk != read_size_disk and read_size_disk != 0:
                disk_plan = 'filter(' + disk_plan + ', '
                disk_plan += ("%.0f" % (decimal.Decimal(real_size_disk) * 100 / read_size_disk)) + '%)'
            io_plan.append(disk_plan)
        if read_size_cache > 0:
            disk_plan = 'file_cache(' + ('%.1f' % read_size_cache) + ' kv, '
            disk_plan += str(len(files_in_cache)) + ' files)'
            if real_size_cache != read_size_cache and read_size_cache != 0:
                disk_plan = 'filter(' + disk_plan + ', '
                disk_plan += ("%.0f" % (decimal.Decimal(real_size_cache) * 100 / read_size_cache)) + '%)'
            io_plan.append(disk_plan)
        if cached_size > 0:
            io_plan.append('write_cache(' + ('%0.1f' % cached_size) + ' kv)')
        io_plan = ' + '.join(io_plan)

        compacting = False

        sorted = self.sorted()
        if self.empty():
            read_plan = 'empty'
        elif sorted == base.Sorted.none:
            self.env.hw.processor.sorter.on_sort(size)
            read_plan = 'sort(' + io_plan + ')'
            compacting = allow_compact
        elif streams > 1:
            self.env.hw.processor.sorter.on_sorted_merge(size)
            read_plan = 'sorted_merge(', io_plan, ', with ', streams, ' streams)'
            compacting = allow_compact
        elif stored_size > 0:
            read_plan = io_plan
        elif cached_size > 0:
            read_plan = io_plan
        else:
            read_plan = '?'

        msg += [' ' * indent, compacting and 'compact' or 'read', ': ', read_plan, ' => ', '%.1f' % size, ' kv\n']

        self.env.log.debug('partition output', '%.1f' % size, 'kv')
        return msg, size, compacting

class DB(db.DB):
    def __init__(self, env, partition_split_size, wal_group_size, first_partition):
        db.DB.__init__(self, env)
        self._partition_split_size = partition_split_size
        self._wal_group_size = wal_group_size

        # [begin, end) for each partition, len(self._boundaries) aways equals to len(self.partitions) + 1
        self._boundaries = [base.Limit.minint, base.Limit.maxint]
        self._partitions = [first_partition]
        self._wal_group_sizes = [1]

    def partition_num(self):
        return len(self._partitions)

    def write(self, k, thread_id):
        db.DB.write(self, k, thread_id)

        self.env.kvs_cache.pop(k, self.env.hw)
        self.env.hw.locks.on_access_obj(self.env.kvs_cache, thread_id)

        i = base.upper_bound(self._boundaries, k) - 1
        self._partitions[i].write(k, thread_id)
        partition = self._partitions[i]
        size_in_file, x = partition.size_in_files()

        self.env.log.debug('write key:', k, 'in-cache:', partition.size_in_cache(),
            'in-file:', '%.1f,' % size_in_file, x)

        j, _ = base.locate_index_in_size_list(self._wal_group_sizes, i)
        self.env.hw.locks.on_access('wal_group_' + str(j), thread_id)

        if partition.size_in_cache() + size_in_file < self._partition_split_size:
            return
        j = self._persist_wal_relate_to_partition(i)
        self._split_partition(i, k)
        self._adjust_wal_groups_after_split(j)

    def _persist_wal_relate_to_partition(self, i):
        ''' collect partitions in the same wal group'''
        j, s = base.locate_index_in_size_list(self._wal_group_sizes, i)
        partitions = map(lambda n: self._partitions[n + s], range(0, self._wal_group_sizes[j]))
        self.env.log.debug('persisting, wal group:', self._wal_group_sizes,
            '<-', i, '=>', j, range(0, self._wal_group_sizes[j]))
        file = db.File(False)
        for partition in partitions:
            partition.persist_write_cache(file)
        return j

    def _adjust_wal_groups_after_split(self, j):
        self._wal_group_sizes[j] += 1
        if self._wal_group_size < 0 or self._wal_group_sizes[j] <= self._wal_group_size:
            return
        s1 = self._wal_group_sizes[j] / 2
        s2 = self._wal_group_sizes[j] - s1
        self._wal_group_sizes[j] = s2
        self._wal_group_sizes.insert(j, s1)

    def _split_partition(self, i, k):
        partition = self._partitions[i]
        prev = (i != 0) and self._partitions[i - 1] or None
        next = (i + 1 < len(self._partitions)) and self._partitions[i + 1] or None
        partitions, split_points = partition.split(self._boundaries[i], self._boundaries[i + 1], prev, next)
        self.env.log.debug('split:', map(lambda x: base.i2s(x), self._boundaries), '<-', k, '=>', split_points)
        if len(split_points) == 0:
            return

        assert self._partitions[i] in partitions, 'splitted partitions should have the origin one, ' + \
            'so that the related files dont need to clean ref-partitions'

        self._partitions.pop(i)
        partitions.reverse()
        for p in partitions:
            self._partitions.insert(i, p)
        split_points.reverse()
        for b in split_points:
            self._boundaries.insert(i + 1, b)

    def file_num(self):
        files = set()
        for partition in self._partitions:
            files = files.union(partition.files())
        return len(files)

    def read(self, k, thread_id, indent = 0, show_id = True):
        db.DB.read(self, k, thread_id, indent, show_id)

        cached = self.env.kvs_cache.access(k, self.env.hw)
        self.env.hw.locks.on_access_obj(self.env.kvs_cache, thread_id)
        if cached:
            return ['cached', '\n'], 0

        if self.env.config.calculate_elapsed:
            self.env.hw.set_snapshot()

        db.DB.read(self, k, thread_id, indent, show_id)
        i = base.upper_bound(self._boundaries, k) - 1
        partition, begin, end = self._partitions[i], self._boundaries[i], self._boundaries[i + 1]
        msg = partition.read(k, begin, end, indent + 2, show_id, False)

        if self.env.config.calculate_elapsed:
            elapsed_sec = self.env.hw.elapsed_sec_from_snapshot(self.env.config.kv_size)
        else:
            elapsed_sec = 0
        return msg, elapsed_sec

    def scan_range(self, begin, end, thread_id, indent = 0, show_id = True):
        db.DB.scan_range(self, begin, end, thread_id, indent, show_id)

        if self.env.config.calculate_elapsed:
            self.env.hw.set_snapshot()
        i = base.upper_bound(self._boundaries, begin) - 1
        j = base.upper_bound(self._boundaries, end) - 1
        msg = []
        size = 0
        for i in range(i, j + 1):
            partition, p_begin, p_end = self._partitions[i], self._boundaries[i], self._boundaries[i + 1]
            p_msg, p_size = partition.scan_range(begin, end, p_begin, p_end, indent + 2, show_id, False)
            msg += p_msg
            size += p_size
        if self.env.config.calculate_elapsed:
            elapsed_sec = self.env.hw.elapsed_sec_from_snapshot(self.env.config.kv_size)
        else:
            elapsed_sec = 0
        return msg, size, elapsed_sec

    def scan_all(self, indent = 0, show_id = True, show_detail = True):
        db.DB.scan_all(self, indent, show_id, show_detail)

        id = lambda obj, name: show_id and name + base.obj_id(obj) or name
        msg = []
        for i in range(0, len(self._partitions)):
            partition, begin, end = self._partitions[i], self._boundaries[i], self._boundaries[i + 1]
            msg += [' ' * indent, 'partition', id(partition, ''), ' #', i]
            msg += [' range[', base.i2s(begin), ', ', base.i2s(end), ')\n']
            msg += partition.scan_all(begin, end, indent + 2, show_id, show_detail)
        return msg

    def status_from_snapshot(self, indent = 0):
        res = db.DB.status_from_snapshot(self, indent)
        res += [
            ' ' * indent, 'Partition info:\n',
            ' ' * indent, '  Partition count: ', self.partition_num(), '\n',
            ' ' * indent, '  File count: ', self.file_num(), '\n']
        return res
