# Sever list, should be hostname instead of ip address.
# The first 3 nodes will deploy "mon + admin + mds + ods", others will only deploy "ods"
export nodes=("localhost" "1" "2" "3")

# Network and mask
export public_network="127.0.0.0/8"

# Deploy user
export user="root"

# Osd loop device image file, should be empty if we use raw device in osd
export osd_img="/data/ceph-osd.img"
# Osd loop device size, means nothing if we use raw device in osd
export osd_mb="204800"

# Osd device name, if osd_img is not empty, this will be the loop device name
export dev_name="/dev/loop0"
