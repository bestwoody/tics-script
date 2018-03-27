# TODO: Find lib path
# *.dyso/*.a path for mac os
export DYLD_LIBRARY_PATH=""
# *.so/*.a path for linux
export LD_LIBRARY_PATH="/usr/local/lib64"

# Executable path
export chbin="build/dbms/src/Server/theflash"

# Server address for scripts
export chserver="127.0.0.1"

# Default database for scripts
export chdb="mutable"
export chdb="default"
