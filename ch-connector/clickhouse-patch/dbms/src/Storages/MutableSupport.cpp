#include <Storages/MutableSupport.h>


namespace DB
{

const std::string MutableSupport::storage_name = "MutableMergeTree";
const std::string MutableSupport::version_column_name = "_INTERNAL_VERSION";
const std::string MutableSupport::delmark_column_name = "_INTERNAL_DELMARK";

// Passed test:
// const DedupCalculator MutableSupport::pipeline_dedup_calculator = DedupCalculatorSyn;
// const bool MutableSupport::in_block_dedup_on_write = false;
// const bool MutableSupport::in_block_dedup_on_read = false;

// Not passed test:
// (mutable-test/delete/delete_one_by_one.test)
// const DedupCalculator MutableSupport::pipeline_dedup_calculator = DedupCalculatorAsynTable;
// const bool MutableSupport::in_block_dedup_on_write = false;
// const bool MutableSupport::in_block_dedup_on_read = false;

// Not passed test:
// (mutable-test/delete/delete_one_by_one.test)
// const DedupCalculator MutableSupport::pipeline_dedup_calculator = DedupCalculatorAsynParallel;
// const bool MutableSupport::in_block_dedup_on_write = false;
// const bool MutableSupport::in_block_dedup_on_read = true;

// Not passed test:
// -- -
//  --
// (mutable-test/dedup/dedup_l116_two_small_parts_g8_o1.test)
//
const DedupCalculator MutableSupport::pipeline_dedup_calculator = DedupCalculatorAsynQueue;
const bool MutableSupport::in_block_dedup_on_write = false;
const bool MutableSupport::in_block_dedup_on_read = true;

}
