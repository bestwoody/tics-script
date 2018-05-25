# How to load data to storage

## Config
* Check config in `_env.sh`

## Loading data
* Manually: `dbgen <table> => trans <table> => import <table>`
* `load.sh <table>` do all the things.
* `load-all.sh` do all the things of all tables

## Scripts
* dbgen.sh: generate table rows
* trans.sh: transform table rows to raw csv formart
* import.sh: load data to storage
* load.sh: dbgen - trans - import
* count.sh: show table count
* drop.sh: drop table
