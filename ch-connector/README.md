# Dev on ClickHouse and keep syncing with the official repo

## First time clone this repo
* `repo/ch-connector> git submodule update`
    * Fetch ClickHouse (and the other submodules)
* `repo/ch-connector> ./clickhouse-init.sh`
    * Fetch ClickHouse submodules

## Developing
* Modify codes on dir `clickhouse`
* `repo/ch-connector> build.sh`
    * Build it

## Commit modifications
* `repo/ch-connector> ./delta-extract.sh`
    * Extract all modifications to dir `delta`
* `repo/ch-connector> git add delta/* && git commit -m "..."`
    * Commit it
* Make sure: do NOT commit modifications in `clickhouse` submodule

## Merge from official repo
* `repo/ch-connector> ./clickhouse-reset.sh`
    * Remove modifications in `clickhouse` submodule
* `repo/ch-connector> git pull ...`
    * Update `clickhouse` submodule from official remote repo
* `repo/ch-connector> git add clickhouse && git commit -m "..."`
    * Commit submodule updates
* `repo/ch-connector> ./delta-apply.sh`
    * Redo all modifications
* `repo/ch-connector> cd clickhouse && git diff`
    * Manually check modifications
