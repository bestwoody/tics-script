# CH Connector notes

## CH wire point
* Patched-CH features
    * Input SQL instructions.
    * Output data.
    * Input compaction instructions.
    * Output compaction status.
    * Data visiblity control, hide uncompacted data (for speed).
    * Data consistency control, hide uncompleted writing data.
    * Data updating supported.
* Can be:
    * Storage layer, best choice when we dont't need CH as coprocesser.
    * AST layer, best choice when we need CH as coprocesser.
    * SQL layer, easiest one.
    * We start with SQL layer.
