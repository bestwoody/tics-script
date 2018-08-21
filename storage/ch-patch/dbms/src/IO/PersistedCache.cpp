#include <IO/PersistedCache.h>

#include <fcntl.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Common/ProfileEvents.h>
#include <IO/BufferWithOwnMemory.h>


namespace ProfileEvents
{
    extern const Event PersistedMarksFileHits;
    extern const Event PersistedMarksFileMisses;
    extern const Event PersistedMarksFileBusy;
    extern const Event PersistedMarksFileUpdate;
    extern const Event PersistedCacheHits;
    extern const Event PersistedCacheMisses;
    extern const Event PersistedCacheExpectedMisses;
    extern const Event PersistedCacheBusy;
    extern const Event PersistedCacheUpdate;
}

namespace DB
{

PersistedCache::PersistedCache(size_t max_size_in_bytes, const std::string & base_path, const std::string & persisted_path)
    : disabled(false), max_size_in_bytes(max_size_in_bytes), base_path(base_path), persisted_path(persisted_path)
{
    log = &Logger::get("PersistedCache");
    if (base_path.at(base_path.size() - 1) != '/')
    {
        LOG_ERROR(log, "DB path should ends with '/': " << base_path << ". Cache disabled");
        disabled = true;
        return;
    }
    if (persisted_path.at(persisted_path.size() - 1) != '/')
        this->persisted_path += '/';

    cleanup_thread = std::make_unique<std::thread>([this]
    {
        while (true)
        {
            std::this_thread::sleep_for(std::chrono::seconds(69));
            try
            {
                size_t n = removeDeletedParts();
                if (n > 0)
                    LOG_TRACE(log, "Cleaned up finish: removed parts count: " << n);
            }
            catch (...)
            {
                LOG_ERROR(log, "Exception when clean up deleted parts: " << getCurrentExceptionMessage(true));
            }
        }
    });

    gc_thread = std::make_unique<std::thread>([this]
    {
        size_t n = 0;
        while (!gc_cancelled)
        {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            n += 1;
            if (n < 119)
                continue;
            n = 0;
            performGC();
        }
    });
}


PersistedCache::~PersistedCache()
{
    // We just leave the cleanup thread gone
    gc_cancelled = true;
    gc_thread->join();
}


bool PersistedCache::redirectMarksFile(std::string & origin_path, size_t file_marks_count)
{
    if (disabled)
        return false;

    std::string cache_path;
    if (!getCachePath(origin_path, cache_path))
        return false;

    PartCacheStatusPtr part_status = getPartCacheStatus(origin_path);

    {
        std::lock_guard<std::mutex> lock(part_status->part_lock);

        FilesMarksCached::iterator file_status_it = part_status->files_marks_cached.find(origin_path);
        if (file_status_it == part_status->files_marks_cached.end())
            file_status_it = part_status->files_marks_cached.emplace(origin_path, FileMarksCached(origin_path, file_marks_count)).first;
        FileMarksCached & marks_status = file_status_it->second;
        if (marks_status.operating_mrk)
        {
            ProfileEvents::increment(ProfileEvents::PersistedMarksFileBusy);
            return false;
        }
    }

    if (Poco::File(cache_path).exists())
    {
        origin_path = cache_path;
        ProfileEvents::increment(ProfileEvents::PersistedMarksFileHits);
        return true;
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::PersistedMarksFileMisses);
        return false;
    }
}


bool PersistedCache::cacheMarksFile(const std::string & origin_path, size_t file_marks_count)
{
    if (disabled)
        return false;

    std::string cache_path;
    if (!getCachePath(origin_path, cache_path))
        return false;

    PartCacheStatusPtr part_status = getPartCacheStatus(origin_path);

    FilesMarksCached::iterator file_status_it;
    {
        std::lock_guard<std::mutex> lock(part_status->part_lock);

        file_status_it = part_status->files_marks_cached.find(origin_path);
        if (file_status_it == part_status->files_marks_cached.end())
            file_status_it = part_status->files_marks_cached.emplace(origin_path, FileMarksCached(origin_path, file_marks_count)).first;
        FileMarksCached & marks_status = file_status_it->second;
        if (marks_status.operating_mrk)
            return false;
        marks_status.operating_mrk = true;
    }
    FileMarksCached & marks_status = file_status_it->second;

    try
    {
        Poco::File origin(origin_path);
        Poco::File part_dir(Poco::Path(cache_path).parent());
        if (!part_dir.exists())
            part_dir.createDirectories();

        if (Poco::File(cache_path).exists())
            return true;

        origin.copyTo(cache_path);
        ProfileEvents::increment(ProfileEvents::PersistedMarksFileUpdate);
    }
    catch (...)
    {
        std::lock_guard<std::mutex> lock(part_status->part_lock);
        marks_status.operating_mrk = false;
        throw;
    }

    std::lock_guard<std::mutex> lock(part_status->part_lock);
    marks_status.operating_mrk = false;
    return true;
}


std::string markRangesToString(const MarkRanges & mark_ranges, const MarksInCompressedFile & marks,
    size_t file_marks_count, bool align = false)
{
    std::stringstream ss;
    ss << "[";
    for (size_t i = 0; i < mark_ranges.size(); ++i)
    {
        size_t right = mark_ranges[i].end;
        if (align && right < file_marks_count && marks[right].offset_in_decompressed_block > 0)
        {
            while (right < file_marks_count &&
                    marks[right].offset_in_compressed_file == marks[mark_ranges[i].end].offset_in_compressed_file)
                ++right;
        }

        for (size_t j = mark_ranges[i].begin; j < right; ++j)
        {
            ss << j;
            if (j + 1 != file_marks_count)
                ss << ",";
        }
    }
    ss << "]";
    return ss.str();
}


bool PersistedCache::redirectDataFile(std::string & origin_path, const MarkRanges & mark_ranges,
    const MarksInCompressedFile & marks, size_t file_marks_count, bool expected_exists)
{
    if (disabled)
        return false;

    std::string cache_path;
    if (!getCachePath(origin_path, cache_path))
        return false;

    if (!Poco::File(cache_path).exists())
    {
        if (expected_exists)
        {
            LOG_TRACE(log, "PersistedCacheMisses, cache file not found, origin: " << origin_path);
            ProfileEvents::increment(ProfileEvents::PersistedCacheMisses);
        }
        else
        {
            ProfileEvents::increment(ProfileEvents::PersistedCacheExpectedMisses);
        }
        return false;
    }

    PartCacheStatusPtr part_status = getPartCacheStatus(origin_path);

    std::lock_guard<std::mutex> lock(part_status->part_lock);

    FilesMarksCached::iterator file_status_it = part_status->files_marks_cached.find(origin_path);
    if (file_status_it == part_status->files_marks_cached.end())
    {
        if (expected_exists)
        {
            LOG_TRACE(log, "PersistedCacheMisses, origin file status not found: " << origin_path);
            ProfileEvents::increment(ProfileEvents::PersistedCacheMisses);
        }
        else
        {
            ProfileEvents::increment(ProfileEvents::PersistedCacheExpectedMisses);
        }
        return false;
    }

    FileMarksCached & marks_status = file_status_it->second;

    if (marks_status.status.size() != file_marks_count)
    {
        LOG_WARNING(log, "Marks count of persisted cache bin file not matched: " << origin_path
            << ", marks count: " << marks_status.status.size() << ", expected: " << file_marks_count);
        if (expected_exists)
            ProfileEvents::increment(ProfileEvents::PersistedCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::PersistedCacheExpectedMisses);
        return false;
    }
    if (marks_status.operating_bin)
    {
        ProfileEvents::increment(ProfileEvents::PersistedCacheBusy);
        return false;
    }

    bool res = isFileMarksAllCached(marks_status, mark_ranges, marks, file_marks_count);
    if (res)
    {
        origin_path = cache_path;
        ProfileEvents::increment(ProfileEvents::PersistedCacheHits);
    }
    else
    {
        if (expected_exists)
        {
            LOG_TRACE(log, "PersistedCacheMisses, not all marks cached: " << origin_path
                << ", required ranges: " << markRangesToString(mark_ranges, marks, file_marks_count)
                << ", aligned ranges: " << markRangesToString(mark_ranges, marks, file_marks_count, true));
            ProfileEvents::increment(ProfileEvents::PersistedCacheMisses);
        }
        else
        {
            ProfileEvents::increment(ProfileEvents::PersistedCacheExpectedMisses);
        }
    }
    return res;
}


bool PersistedCache::cacheMarkRangesInDataFile(const std::string & origin_path, const MarkRanges & mark_ranges,
    const MarksInCompressedFile & marks, size_t file_marks_count, size_t max_buffer_size)
{
    if (disabled)
        return false;

    PartOriginPath part_path = Poco::Path(origin_path).parent().toString();
    std::string cache_path;
    if (!getCachePath(origin_path, cache_path))
        return false;

    PartCacheStatusPtr part_status = getPartCacheStatus(origin_path);

    FilesMarksCached::iterator file_status_it;
    {
        std::lock_guard<std::mutex> lock(part_status->part_lock);

        file_status_it = part_status->files_marks_cached.find(origin_path);
        if (file_status_it == part_status->files_marks_cached.end())
            file_status_it = part_status->files_marks_cached.emplace(origin_path, FileMarksCached(origin_path, file_marks_count)).first;
        FileMarksCached & marks_status = file_status_it->second;

        if (marks_status.operating_bin)
            return false;
        if (isFileMarksAllCached(marks_status, mark_ranges, marks, file_marks_count))
            return true;
        marks_status.operating_bin = true;
    }
    FileMarksCached & marks_status = file_status_it->second;

    try
    {
        if (!copyFileRanges(origin_path, cache_path, mark_ranges, marks, file_marks_count, max_buffer_size))
            return false;
    }
    catch (...)
    {
        std::lock_guard<std::mutex> lock(part_status->part_lock);
        marks_status.operating_bin = false;
        throw;
    }

    {
        std::lock_guard<std::mutex> lock(part_status->part_lock);

        for (size_t i = 0; i < mark_ranges.size(); ++i)
        {
            size_t right = mark_ranges[i].end;
            if (right < file_marks_count && marks[right].offset_in_decompressed_block > 0)
            {
                while (right < file_marks_count &&
                    marks[right].offset_in_compressed_file == marks[mark_ranges[i].end].offset_in_compressed_file)
                    ++right;
            }

            for (size_t j = mark_ranges[i].begin; j < right; ++j)
                marks_status.status[j] = 1;
        }

        marks_status.operating_bin = false;
    }

    ProfileEvents::increment(ProfileEvents::PersistedCacheUpdate);
    return true;
}


PersistedCache::PartCacheStatusPtr PersistedCache::getPartCacheStatus(const std::string & origin_path)
{
    std::string part_path = Poco::Path(origin_path).parent().toString();

    std::lock_guard<std::mutex> lock(cache_lock);
    CacheStatus::iterator part_status_it = cache_status.find(part_path);
    if (part_status_it == cache_status.end())
        part_status_it = cache_status.emplace(part_path, std::make_shared<PartCacheStatus>(part_path)).first;

    return part_status_it->second;
}


bool PersistedCache::getCachePath(const std::string & origin_path, std::string & cache_path)
{
    cache_path = persisted_path + (origin_path.c_str() + base_path.size());
    if (strncmp(base_path.c_str(), origin_path.c_str(), base_path.size()) != 0)
    {
        LOG_ERROR(log, "Data file doesn't have the same prefix of db path: " << base_path << ", " << origin_path << ". Cache disabled");
        disabled = true;
        return false;
    }
    return true;
}


bool PersistedCache::isFileMarksAllCached(const FileMarksCached & marks_status, const MarkRanges & mark_ranges,
    const MarksInCompressedFile & marks, size_t file_marks_count)
{
    for (size_t i = 0; i < mark_ranges.size(); ++i)
    {
        size_t right = mark_ranges[i].end;
        if (right < file_marks_count && marks[right].offset_in_decompressed_block > 0)
        {
            while (right < file_marks_count &&
                marks[right].offset_in_compressed_file == marks[mark_ranges[i].end].offset_in_compressed_file)
                ++right;
        }

        for (size_t j = mark_ranges[i].begin; j < right; ++j)
            if (!marks_status.status[j])
                return false;
    }
    return true;
}


bool PersistedCache::copyFileRanges(const std::string & origin_path, const std::string & cache_path,
    const MarkRanges & mark_ranges, const MarksInCompressedFile & marks, size_t file_marks_count, size_t max_buffer_size)
{
    Memory memory(max_buffer_size);

    int fd_r = ::open(origin_path.c_str(), O_RDONLY);
    if (0 > fd_r)
    {
        LOG_ERROR(log, "Origin file can't be open while copying to persisted cache: " << origin_path << ", errno: " << errno);
        return false;
    }
    int fd_w = ::open(cache_path.c_str(), O_WRONLY | O_TRUNC | O_CREAT, 0644);
    if (0 > fd_w)
    {
        LOG_ERROR(log, "Cache file can't be open while copying to persisted cache: " << cache_path << ", errno: " << errno);
        return false;
    }

    bool res = true;
    for (size_t i = 0; i < mark_ranges.size(); ++i)
    {
        size_t right = mark_ranges[i].end;
        if (right < file_marks_count && marks[right].offset_in_decompressed_block > 0)
        {
            while (right < file_marks_count &&
                marks[right].offset_in_compressed_file == marks[mark_ranges[i].end].offset_in_compressed_file)
                ++right;
        }

        size_t begin = marks[mark_ranges[i].begin].offset_in_compressed_file;
        size_t size = ((right < file_marks_count) ? marks[right].offset_in_compressed_file : Poco::File(origin_path).getSize()) - begin;

        if (!copyFileRange(origin_path, cache_path, fd_r, fd_w, begin, size, memory.data(), memory.size()))
        {
            res = false;
            break;
        }
    }

    ::close(fd_r);
    ::fsync(fd_w);
    ::close(fd_w);

    return res;
}


bool PersistedCache::copyFileRange(const std::string & origin_path, const std::string & cache_path,
    int fd_r, int fd_w, size_t pos, size_t size, char * buffer, size_t buffer_size)
{
    while (size > 0)
    {
        size_t n = std::min(size, buffer_size);
        size_t res = ::pread(fd_r, buffer, n, pos);
        if (n != res)
        {
            LOG_ERROR(log, "Origin file can't be read while copying to persisted cache: " << origin_path <<
                ", pos: " << pos << ", size: " << n << ", errno: " << errno);
            return false;
        }

        res = ::pwrite(fd_w, buffer, n, pos);
        if (n != res)
        {
            LOG_ERROR(log, "cache file can't be written while copying to persisted cache: " << cache_path <<
                ", pos: " << pos << ", size: " << n << ", errno: " << errno);
            return false;
        }

        size -= n;
        pos += n;
    }

    return true;
}


void PersistedCache::performGC()
{
    try
    {
        scanExpiredParts();
        evictMostUnusedParts();
    }
    catch (...)
    {
        LOG_ERROR(log, "Exception when perform GC: " << getCurrentExceptionMessage(true));
    }
}


void PersistedCache::deletePart(const std::string & cache_path)
{
    Poco::Path path(cache_path);
    std::string dir = path.parent().toString();
    std::string name = path.getBaseName();
    std::string new_path = dir + DeletedDirPrefix + name;

    LOG_TRACE(log, "Deleting file: " << cache_path << " => " << new_path);
    Poco::File(cache_path).renameTo(new_path);
}


size_t PersistedCache::removeDeletedParts()
{
    size_t removed_count = 0;

    std::string root_path = persisted_path + "data";
    Poco::File root(root_path);
    if (!root.exists())
        return 0;

    std::vector<std::string> dbs;
    root.list(dbs);

    for (auto db: dbs)
    {
        std::string db_path = root_path + "/" + db;
        std::vector<std::string> tables;
        Poco::File(db_path).list(tables);

        for (auto table: tables)
        {
            std::string table_path = db_path + "/" + table;
            std::vector<std::string> parts;
            Poco::File(table_path).list(parts);

            for (auto part: parts)
            {
                if (strncmp(part.c_str(), DeletedDirPrefix.c_str(), DeletedDirPrefix.size()) == 0)
                {
                    removed_count += 1;
                    std::string cache_path = table_path + "/" + part;
                    LOG_TRACE(log, "Removing deleted part: " << cache_path);
                    Poco::File(cache_path).remove(true);
                }
            }
        }
    }

    return removed_count;
}


void PersistedCache::scanExpiredParts()
{
    std::string root_path = "data";
    Poco::File root(persisted_path + root_path);
    if (!root.exists())
        return;

    std::vector<std::string> dbs;
    root.list(dbs);

    for (auto db: dbs)
    {
        std::string db_path = root_path + "/" + db;
        std::vector<std::string> tables;
        Poco::File(persisted_path + db_path).list(tables);

        for (auto table: tables)
        {
            std::string table_path = db_path + "/" + table;
            std::vector<std::string> parts;
            Poco::File(persisted_path + table_path).list(parts);

            for (auto part: parts)
            {
                std::string origin_path = base_path + table_path + "/" + part;
                if (!Poco::File(origin_path).exists() &&
                    strncmp(part.c_str(), DeletedDirPrefix.c_str(), DeletedDirPrefix.size()) != 0)
                {
                    std::string cache_path = persisted_path + table_path + "/" + part;
                    deletePart(cache_path);
                }
            }
        }
    }
}


void PersistedCache::evictMostUnusedParts()
{
    // TODO: impl
}

}
