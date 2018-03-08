#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

BlockInputStreams mergeMutableBlockInputStreams(BlockInputStreams inputs, const SortDescription & description,
    const String & version_column, size_t max_block_size);

}
