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

* In `<repo>/storage/ch`, which is the submodule of this repo, do:
    - Checkout a new branch, e.g. `branch_a` in this repo.
    - Do some modifications, and submit some commits to `branch_a`.
    - Push `branch_a` of `<repo>/storage/ch` to git server after done your job.
* In `<repo>` directory, do:
    - Checkout a new branch, e.g. `branch_a` in this repo.
    - Run `git add storage/ch` command to track commit changes in `<repo>/storage/ch`.
    - Submit to `branch_a` and push to git server.
* After review passed, merge two branches called `branch_a` in `<repo>/storage/ch` and in `<repo>` to master.
