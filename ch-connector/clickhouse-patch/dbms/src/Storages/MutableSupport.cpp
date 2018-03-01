#include <Storages/MutableSupport.h>


namespace DB
{

const std::string MutableSupport::mutable_storage_name = "MutableMergeTree";
const std::string MutableSupport::mutable_version_column_name = "_INTERNAL_VERSION";
const std::string MutableSupport::mutable_delmark_column_name = "_INTERNAL_DELMARK";

}
