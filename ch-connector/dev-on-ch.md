# Dev on CH and keep syncing with the official repository

## First time clone this repository
* `repo/ch-connector> git submodule update --init --recursive`
    * Fetch CH (and the other submodules)
    * Fetch CH submodules

## Developing
* Modify codes on dir `ch`
* `repo/ch-connector> ./build.sh`
    * Build it

## Commit modifications
* `repo/ch-connector> ./patch-extract.sh`
    * Extract all modifications to dir `ch-patch`
* `repo/ch-connector> git add ch-patch/* && git commit -m "..."`
    * Commit it
    * If there are removed or relocated files, we should delete it in `ch-patch/...` manually, before Committing
* Make sure: do NOT commit modifications in `ch` submodule

## Merge from official repository
* `repo/ch-connector> ./ch-repo-reset.sh`
    * Remove modifications in `ch` submodule
* `repo/ch-connector> cd ch && git pull ...`
    * Update `ch` submodule from official remote repository
* `repo/ch-connector> git add ch && git commit -m "..."`
    * Commit submodule updates
* `repo/ch-connector> ./patch-apply.sh`
    * Redo all modifications
* `repo/ch-connector> cd ch && git diff`
    * Manually check modifications
