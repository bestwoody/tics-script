#pragma once

#include "Chan.h"
#include "ArrowEncoder.h"

namespace Magic
{

// TODO: Reorder blocks, some times may faster
class ArrowEncoderParall
{
public:
    ArrowEncoderParall(const std::string & error) : encoder(error) {}

    ArrowEncoderParall(DB::BlockIO & result, size_t conc) : encoder(result), encodeds(-1, conc * 1)
    {
        if (encoder.hasError())
            return;

        encoders.resize(conc);
        for (auto it = encoders.begin(); it != encoders.end(); ++it)
        {
            *it = std::thread([&]
            {
                while (true)
                {
                    auto origin = encoder.getOriginBlock();
                    auto block = encoder.serializedBlock(encoder.encodeBlock(origin));
                    if (block)
                    {
                        encodeds.push(block);
                    }
                    else
                    {
                        encodeds.setQuota(encoder.blocks());
                        break;
                    }
                }
            });
        }
    }

    bool residue()
    {
        return encodeds.size();
    }

    inline bool hasError()
    {
        return encoder.hasError();
    }

    std::string getErrorString()
    {
        return encoder.getErrorString();
    }

    ArrowEncoder::BufferPtr getEncodedSchema()
    {
        return encoder.getEncodedSchema();
    }

    ArrowEncoder::BufferPtr getEncodedBlock()
    {
        ArrowEncoder::BufferPtr block;
        encodeds.pop(block);
        return block;
    }

    ~ArrowEncoderParall()
    {
        // Don't check encodeds.size(), in case query is cancalled.
        // std::cerr << "ArrowEncoderParall: threads finishing" << std::endl;
        for (auto it = encoders.begin(); it != encoders.end(); ++it)
            it->join();
        // std::cerr << "ArrowEncoderParall: threads finished" << std::endl;
    }

private:
    ArrowEncoder encoder;

    Chan<ArrowEncoder::BufferPtr> encodeds;
    std::vector<std::thread> encoders;
};

}
