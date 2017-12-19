#pragma once

#include <Common/ConcurrentBoundedQueue.h>
#include "ArrowEncoder.h"

namespace Magic
{

// Simple async decoding: use a separated thread
// TODO: One thread is not fast enought, it's about 0.5GB/s (one core)
// TODO: More threads: spawn decoding threads hear, then reorder blocks
class AsyncArrowEncoder : public ArrowEncoder
{
public:
    AsyncArrowEncoder(const std::string & error) : ArrowEncoder(error), encodeds(32) {}

    AsyncArrowEncoder(DB::BlockIO & result) : ArrowEncoder(result), encodeds(32)
    {
        if (hasError())
            return;

        // TODO: concurrent count
        encoders.resize(1);
        for (auto it = encoders.begin(); it != encoders.end(); ++it)
        {
            *it = std::thread([&]
            {
                while (true)
                {
                    auto block = ArrowEncoder::getEncodedBlock();
                    encodeds.push(block);
                    if (!block)
                        break;
                }
            });
        }

        // TODO: thread.join, when we support cancallation.
    }

    BufferPtr getPreparedEncodedBlock()
    {
        BufferPtr block;
        encodeds.pop(block);
        return block;
    }

    ~AsyncArrowEncoder()
    {
        // Don't check encodeds.size(), in case query is cancalled.
        for (auto it = encoders.begin(); it != encoders.end(); ++it)
            it->join();
    }

private:
    BufferPtr getEncodedBlock();

    ConcurrentBoundedQueue<BufferPtr> encodeds;
    std::vector<std::thread> encoders;
};

}
