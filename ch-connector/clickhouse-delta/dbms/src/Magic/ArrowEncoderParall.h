#pragma once

#include "Chan.h"
#include "ArrowEncoder.h"

namespace Magic
{

// TODO: Reorder blocks, some times may faster
class ArrowEncoderParall : public ArrowEncoder
{
public:
    ArrowEncoderParall(const std::string & error) : ArrowEncoder(error) {}

    ArrowEncoderParall(DB::BlockIO & result, size_t conc) : ArrowEncoder(result), encodeds(0, conc * 1)
    {
        if (hasError())
            return;

        encoders.resize(conc);
        for (auto it = encoders.begin(); it != encoders.end(); ++it)
        {
            *it = std::thread([&]
            {
                while (true)
                {
                    auto block = ArrowEncoder::getEncodedBlock();
                    if (block)
                    {
                        encodeds.push(block);
                    }
                    else
                    {
                        encodeds.setQuota(ArrowEncoder::blocks());
                        break;
                    }
                }
            });
        }
    }

    BufferPtr getPreparedEncodedBlock()
    {
        BufferPtr block;
        encodeds.pop(block);
        return block;
    }

    ~ArrowEncoderParall()
    {
        // Don't check encodeds.size(), in case query is cancalled.
        for (auto it = encoders.begin(); it != encoders.end(); ++it)
            it->join();
    }

private:
    BufferPtr getEncodedBlock();

    Chan<BufferPtr> encodeds;
    std::vector<std::thread> encoders;
};

}
