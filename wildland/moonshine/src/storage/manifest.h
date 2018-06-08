#pragma once

#include <memory>

#include "fs/local.h"
#include "persist/meta.h"
#include "storage/storages.h"
#include "storage/null/null.h"
#include "storage/plain/plain.h"

//                                                     -+
//                            +--------------------+    |
//                            | ITablesLayout      |    |
//                            | +----------------+ |    | Data layout interface
//     +-------------------+  | | IColumnsLayout | |    |
//     | ICreateSqlPersist |  | +----------------+ |    |
//     +-------------------+  +--^-----------------+   -+
//              ^                |                     -+
//              |             IBlockPersist             |
//              |                |                      |
//              |             IStorage                  | Data operating strategy
//              |                |                      |
//              +------------ Storages                  |
//                               |                     -+
//  - - - - - - - - - - - - - - -|- - - - - - - - - - - -
//                               |
//              +----------------v-----------+
//              | IFS(file system interface) |
//              +----------------------------+
//

namespace moonshine {

using std::make_shared;

StoragesPtr CreateStoragesSimple(const string &path) {
    FSPtr fs = make_shared<FSLocalSync>();
    CreateSqlPersistPtr meta_persist = make_shared<CreateSqlPersistSingleFile>(fs);
    StoragesPtr storages = make_shared<Storages>(fs, meta_persist, path);

    storages->Regester(make_shared<StorageNull>());

    TablesLayoutPtr layout = make_shared<TablesLayoutByDir<ColumnsLayoutByBlock>>(fs, path);
    BlockPersistPtr block_persist = make_shared<BlockPersistSync>(fs);
    storages->Regester(make_shared<StoragePlain>(layout, block_persist));

    storages->LoadTables();
    return storages;
}

}
