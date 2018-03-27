CH is fast, may be the fastest.  
But it has limit, it lack of data mutation supporting.  
So here we are, injecting the magic dose to the giant.  

# Aim for:
* Support UPSERT/DELETE grammar.
* NOT drag the speed down, espectially in SELECT.

# Plan:
* Provide a new table engine MutableMergeTree.
* In merging it behave excactly like ReplacingMergeTree.
* In table creating it acts like ReplacingMergeTree, diff in:
  * No need to assign a version column, it's implied
  * A new table engine param for version generating method as above
* In data inserting it acts like all the MergeTree family, diff in:
  * The hidden implied column,
    it's value provide by local-machine-timestamp
  * Or by an increasing-version-service
* In data reading(SELECT mostly) it acts like ReplacingMergeTree in merging:
  * Dedup rows by prime key(s)
  * Choose the higher version row as result
  * Random result when meet more than one highest verson rows
* Add another hidden column for deletion marks.
  * Also implied, create and maintain by the table engine.
  * Insert/write/merge data as normal
    Filter rows by del-mark when reading after dedup

# Side effects:
* Read all prime key columns and version column for dedup in any case.  
  Nothing we can do about this read amplification (yet).
* Deduplicating rows may slighty slow down the data reading.  
  Should make sure it's minor.
* Using an increasing-version-service may slow down data inserting.  
  Should make sure it's minor.

# Progress:
```
[*****] Merging rows on read
[*****] New table engine: MutableMergeTree
[*****] Auto create version and del-mark columns for MutableMergeTree
[*****] Version column value auto gen: local
[*****] New grammar IMPORT, write all columns (include implied columns)
[*****] New grammar SELRAW (select raw), read all columns
[*****] New grammar DELETE, delecting rows on read
[-----] Integration testing, speed comparing, etc
```

# Bring it online:
```
[-----] Clear all TODOs in Injections below
[-----] Support cluster engine
[-----] Version column value auto gen: extra service
[-----] ...
```

# Injections:
List all the changes we made, prevent to lose trace in the frequently code merging.

#### Merging on read
```
Storage/StorageMergeTree::read
    This method is the raw data reading's entry of MergeTree family
    Use our streams to wrap the DataInputStreams
        Dedup rows by sorted prime key(s)
        TODO: Filter rows by del-mark
```
```
Storages/MergeTree/MergeTreeDataSelectExecutor::read
    This method is the query entry of MergeTree family
    This class read all relate data for the SQL query
    Force read prime key columns and version column if meet our engine type
    May sure every Block of this table has these columns' info so it can be sorted and deduped
```
```
DataStreams/mergeMutableBlockInputStreams
    Impliment merge rows
```

#### New table engine MutableMergeTree
```
Storage/StorageFactory::get
    This method is the entry of table instance creating
    Create MutableMergeTree when match the engine name
```
```
Storages/MergeTree/MergeTreeData::MergingParams
Storages/MergeTree/MergeTreeData::supportsFinal
Storages/MergeTree/MergeTreeData::getModeName
    Define MutableMergeTree enum item
```
```
Storages/MergeTree/MergeTreeDataMerger::mergePartsToTemporaryPart
Storages/MergeTree/MergeTreeDataMerger::reshardPartition
    Act as ReplacingMergeTree when merge
```
```
Storages/MergeTree/MergeTreeDataSelectExecutor::spreadMarkRangesAmongStreamsFinal
    Act as ReplacingMergeTree when read
```
```
Storages/MergeTree/MergeTreeDataSelectExecutor::read
    Read all prime key columns and version column for dedup
    TODO: check if 'sampling query' works fine
```

#### Auto create hidden implied columns for MutableMergeTree
A little tricky: diffent mods should saw diffent table schema
```
Storages/HiddenColumns
    Define hidden columns
```
```
Storage/StorageFactory::get
    Create version and del-mark columns when creating table
    Link version column to engine params
    TODO: help string
```
```
DataStreams/AdditionalColumnsBlockOutputStream
Storages/StorageMergeTree::getVersionSeed
Storage/StorageMergeTree::write
    Auto gen values for version and del-mark columns when write
    TODO: disable CH default value columns creating
```
```
Storages/ITableDeclaration::getHiddenColumnsImpl
Storages/ITableDeclaration::getSampleBlockNoHidden
Storages/ITableDeclaration::getSampleBlockNonMaterializedNoHidden
Storages/MergeTree/MergeTreeData::getHiddenColumnsImpl
Interpreters/InterpreterInsertQuery::getSampleBlockNoHidden
    Hide the version and del-mark columns from INSERT query parse
    So that the input data can be parse in the right format.
    TODO: lack of tests about: materialized, INSERT...SELECT
```
```
Interpreters/ExpressionAnalyzer::normalizeTreeImpl
    Filter version and del-mark columns when SELECT *
```
```
DataStreams/filterColumnsBlockInputStreams
Storage/StorageMergeTree::read
    Use filter streams to wrap the DataInputStreams
    Filter version and del-mark columns' data when SELECT
```
```
Interpreters/InterpreterDescribeQuery::executeImpl
    Filter version and del-mark columns when DESC table
```
```
Interpreters/InterpreterAlterQuery::parseAlter
    Pervent modifications of version and del-mark columns when alter table
    TOOD: test altering partition, check if there are werid things
```

#### New grammar IMPORT and UPSERT
```
Parsers/ASTInsertQuery
Parsers/ParserInsertQuery::parseImpl
    Gramma parsing support for IMPORT / UPSERT
    Add "ASTInsertQuery.is_import" mark
```
```
Storages/StorageMergeTree::write
    If is importing, do not add extra columns
```
```
Interpreters/InterpreterInsertQuery::getSampleBlock
    This method create sample block as format instructor
    if is importing, create block with all columns
    if is inserting, create block without version and del-mark columns
```

#### New grammar SELRAW (SELect RAW)
TODO: support SELRAW in: "CREATE...SELRAW", "INSERT...SELRAW"
```
Parsers/ASTSelectQuery
Parsers/ParserSelectQuery::parseImpl
    Gramma parsing support for SELRAW
    Add "ASTSelectQuery.raw_for_mutable" mark
```
```
Storages/StorageMergeTree::read
    If is importing, do not dedup rows
```
```
Interpreters/ExpressionAnalyzer::normalizeTreeImpl
    Do not filter version and del-mark columns when SELRAW *
```

#### New grammar DELETE
```
Common/ProfileEvents
    Add profile event "DeleteQuery"
```
```
Parsers/ASTDeleteQuery
Parsers/ParserDeleteQuery
Interpreters/InterpreterDeleteQuery
    Add DELETE grammar
```
```
Interpreters/InterpreterFactory::get
Interpreters/ParserQuery::parseImpl
    Intergrade DELETE grammar
```
```
Storages/StorageMergeTree::write
    Gen version and del-mark columns when DELETE
```
