#pragma once

#include "QuotaChan.h"
#include "ArrowEncoder.h"

namespace TheFlash
{

class ArrowEncoderParall
{
public:
    ArrowEncoderParall(const std::string & error) : encoder(error)
    {
    }

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
                    auto block = encoder.serializeBlock(encoder.encodeBlock(origin));
                    if (block)
                    {
                        encodeds.push(block);
                    }
                    else
                    {
                        size_t blocks = encoder.blocks();
                        blocks = (blocks != SafeBlockIO::UnknownSize) ? blocks : 0;
                        encodeds.setQuota(blocks);
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

    void cancal(bool exception)
    {
        encoder.cancal(exception);
    }

    ~ArrowEncoderParall()
    {
        encodeds.close();
        for (auto it = encoders.begin(); it != encoders.end(); ++it)
            it->join();
    }

private:
    ArrowEncoder encoder;

    QuotaChan<ArrowEncoder::BufferPtr> encodeds;
    std::vector<std::thread> encoders;
};

}
