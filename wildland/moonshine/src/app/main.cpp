#include <iostream>

#include "base/args.h"
#include "stream/random.h"
#include "storage/storages.h"

using std::string;

using std::cout;
using std::cerr;
using std::endl;

using std::min;

using moonshine::Err;
using moonshine::ErrWrongUsage;

using moonshine::Schema;
using moonshine::UnidirectSortDesc;

using moonshine::GenRandBlock;
using moonshine::StreamRandom;

using moonshine::Args;
using moonshine::BlocksInputPtr;
using moonshine::TablePtr;
using moonshine::Storages;

int CmdRandWrite(int argc, const char *argv[]) {
    if (argc != 4) {
        cerr << "usage: <bin> randwrite store-path screate-sql block-rows total-rows" << endl;
        return -1;
    }

    string path(argv[0]);
    Storages storages(path);

    string create_sql(argv[1]);
    TablePtr table = storages.CreateTable(create_sql);

    size_t block_rows((size_t)atoi(argv[2]));
    size_t total_rows((size_t)atoi(argv[3]));
    size_t prepare_blocks = (total_rows + block_rows - 1) / block_rows;
    size_t max_prepare_blocks = 512;

    GenRandBlock gen_blocks(table->GetSchema(), block_rows, min(prepare_blocks, max_prepare_blocks));
    StreamRandom blocks(gen_blocks, total_rows);

    table->Write(blocks);
    return 0;
}

int CmdScan(int argc, const char *argv[]) {
    if (argc != 2) {
        cerr << "usage: <bin> scan store-path table-name" << endl;
        return -1;
    }

    string path(argv[0]);
    Storages storages(path);

    string name(argv[1]);
    TablePtr table = storages.GetTable(name);

    BlocksInputPtr blocks = table->Scan();
    while (!blocks->Done())
        blocks->Read().DebugPrintByColumn(cout, table->GetSchema());
    return 0;
}

int main(int argc, const char *argv[]) {
    --argc;
    ++argv;

    if (argc < 1) {
        cerr << "usage: <bin> randwrite|scan ..." << endl;
        return -1;
    }

    string sub(argv[0]);
    --argc;
    ++argv;

    int ret_code = 0;

    try {
        if (sub == "randwrite")
            ret_code = CmdRandWrite(argc, argv);
        else if (sub == "scan")
            ret_code = CmdScan(argc, argv);
        else
            throw ErrWrongUsage("no matched sub command, got: " + sub);
    } catch (Err e) {
        cerr << e << endl;
        return -1;
    } catch (...) {
        cerr << "unknown exception" << endl;
        return -1;
    }

    return ret_code;
}
