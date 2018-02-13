#include <DataStreams/filterColumnsBlockInputStreams.h>
#include <common/logger_useful.h>

namespace DB
{

Block FilterColumnsBlockInputStream::readImpl()
{
    Block block = input->read();
    if (!block)
        return block;
    for (auto it: names)
        if (block.has(it))
            block.erase(it);
    return block;
}


BlockInputStreams filterColumnsBlockInputStreams(BlockInputStreams inputs,
    const SortDescription & description,
    const String & version_column,
    size_t max_block_size,
    const NameSet & filter_names)
{
    BlockInputStreams res(inputs.size());
    for (size_t i = 0; i < inputs.size(); ++i)
        res[i] = std::make_shared<FilterColumnsBlockInputStream>(inputs[i], filter_names);
    return res;
}

}
