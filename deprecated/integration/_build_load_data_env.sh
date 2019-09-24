#!/bin/bash

set -ue
set -o pipefail

source ./_cluster_env.sh

cat<<EOF>./load_data/_env_load.sh
#!/bin/bash
# This is a generated file. Do not edit it. Changes will be overwritten.

#!/bin/bash

export scale="${scale}"
export use_pk="false"
export blocks="16"

export schema="./schema"
export data_src="/data1/data/tpch"

export mysql_host=${storage_server[0]}
export mysql_port=${tidb_port}

EOF
