# Sever list, should be hostname instead of ip address.
# The first 3 nodes will deploy "mon + admin + mds + ods", others will only deploy "ods"
export nodes=("localhost")

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

# Ceph repo url for ceph-deploy
#export repo_url="http://mirrors.163.com/ceph/rpm-mimic/el7"
#export repo_url="http://download.ceph.com/ceph/rpm-mimic/el7"
#export repo_url="http://mirrors.shu.edu.cn/ceph/rpm-mimic/el7"
export repo_url="http://mirrors.aliyun.com/ceph/rpm-mimic/el7"
export repo_gpg="https://download.ceph.com/keys/release.asc"

# Override ceph repo url
# TODO: use local repo
# This is a weird behavior of ceph-deploy:
#   0. There are 3 different ways to specify the ceph repo url:
#      a. The yum config file, normally is /etc/yum.repos.d/ceph.repo
#      b. The '--repo-url' arg of command 'ceph-deploy'
#      c. The environment var 'CEPH_DEPLOY_REPO_URL'
#   1. The yum config will be totally ignored and overwritten
#      a. We still need this repo file for installing ceph-deploy, after that it's useless
#      b. This file will be rewritten by '--repo-url' or 'CEPH_DEPLOY_REPO_URL' after command 'ceph-deploy'
#   2. 'CEPH_DEPLOY_REPO_URL' always win
#      a. See: /usr/lib/python2.7/site-packages/ceph_deploy/install.py#160 (ceph-deploy v2.0)
#      b. Default url 'http://download.ceph.com/rpm-jewel/el7'
#      c. If it's set, use it
#      d. If it not exists:
#         A. If '--repo-url' exists, use '--repo-url',
#            but may fallback to default url in some sub-commad called by ceph-deploy, seems it's a bug
#         B. If '--repo-url' not exists, fallback to default value, but not use yum config
export CEPH_DEPLOY_REPO_URL="$repo_url"
export CEPH_DEPLOY_GPG_URL="$repo_gpg"
