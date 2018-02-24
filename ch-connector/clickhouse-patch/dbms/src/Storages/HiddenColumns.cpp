#include <Storages/HiddenColumns.h>


namespace DB
{

const std::string HiddenColumns::mutable_version_column_name = "_INTERNAL_VERSION";
const std::string HiddenColumns::mutable_delmark_column_name = "_INTERNAL_DELMARK";

}
