# Dev on ClickHouse and keep syncing with the official repo

## First time clone this repo
* `repo/ch-connector> git submodule update`
    * Fetch ClickHouse (and the other submodules)
* `repo/ch-connector> cd clickhouse && clickhouse-init.sh`
    * Fetch ClickHouse submodules

## Developing
* Modify codes on dir `clickhouse`
* `repo/ch-connector> build.sh`
    * Build it

## Commit modifications
* `repo/ch-connector> delta-extract.sh`
    * Extract all modifications to dir `delta`
* `repo/ch-connector> git add delta/* && git commit -m "..."`
    * Commit it
