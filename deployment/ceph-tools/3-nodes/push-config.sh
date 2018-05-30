set -eu
source ./_env.sh
ceph-deploy --username "$user" --overwrite-conf config push "$h0" "$h1" "$h2"
