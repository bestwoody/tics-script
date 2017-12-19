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

    AsyncArrowEncoder(const DB::BlockIO & result) : ArrowEncoder(result), encodeds(32)
    {
        if (hasError())
            return;

        vector<thread> encoders(4);
        for (auto it = loaders.begin(); it != loaders.end(); ++it)
        {
            *it = thread([&] {
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
        thread.join();
    }

private:
    BufferPtr getEncodedBlock();

    ConcurrentBoundedQueue<BufferPtr> encodeds;
    std::thread encoders;
};

}
