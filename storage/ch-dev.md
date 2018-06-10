# Dev on CH and keep syncing with the official repository

## First time clone this repository
* `repo/storage> git submodule update --init --recursive`
    * Fetch CH (and the other submodules)
    * Fetch CH submodules

## Developing
* Modify codes on dir `ch`
* `repo/storage> ./build.sh`
    * Build it

## Commit modifications
* `repo/storage> ./patch-extract.sh`
    * Extract all modifications to dir `ch-patch`
* `repo/storage> git add ch-patch/* && git commit -m "..."`
    * Commit it
    * If there are removed or relocated files, we should delete it in `ch-patch/...` manually, before Committing
* Make sure: do NOT commit modifications in `ch` submodule

## Merge from official repository
* `repo/storage> ./ch-repo-reset.sh`
    * Remove modifications in `ch` submodule
* `repo/storage> cd ch && git pull ...`
    * Update `ch` submodule from official remote repository
* `repo/storage> git add ch && git commit -m "..."`
    * Commit submodule git-hash update
* `repo/storage> ./patch-apply.sh`
    * Redo all modifications
* `repo/storage> cd ch && git diff`
    * Manually check modifications
