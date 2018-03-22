#include <Storages/MutableSupport.h>


namespace DB
{

const std::string MutableSupport::storage_name = "MutableMergeTree";
const std::string MutableSupport::version_column_name = "_INTERNAL_VERSION";
const std::string MutableSupport::delmark_column_name = "_INTERNAL_DELMARK";

const size_t MutableSupport::default_partition_num = 16;

const MutableSupport::DeduperType MutableSupport::deduper = MutableSupport::DeduperDedupPartitioning;

}
