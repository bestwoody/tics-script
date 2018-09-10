#!/bin/bash

# Ceph repo url for ceph-deploy
#export repo_url="http://mirrors.163.com/ceph/rpm-mimic/el7"
#export repo_url="http://download.ceph.com/ceph/rpm-mimic/el7"
#export repo_url="http://mirrors.shu.edu.cn/ceph/rpm-mimic/el7"
export repo_url="http://mirrors.aliyun.com/ceph/rpm-mimic/el7"
#export repo_gpg="https://download.ceph.com/keys/release.asc"
export repo_gpg="file://`readlink -f ../download/downloaded/release.asc`"

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

confirm()
{
	read -p "=> hit enter to continue"
}
export conform
