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
    AsyncArrowEncoder(const std::string & error) : ArrowEncoder(error), encodeds(3) {}

    AsyncArrowEncoder(const DB::BlockIO & result) : ArrowEncoder(result), encodeds(5)
    {
        if (hasError())
            return;

        thread = std::thread([&]
        {
            while (true)
            {
                auto block = ArrowEncoder::getEncodedBlock();
                encodeds.push(block);
                if (!block)
                    break;
            }
        });

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
    std::thread thread;
};

}
