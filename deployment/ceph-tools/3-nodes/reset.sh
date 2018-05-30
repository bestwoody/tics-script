cho ceph-deploy purge ip-172-16-30-2 ip-172-16-30-3 ip-172-16-30-4
ceph-deploy --username "pingcap" purge ip-172-16-30-2 ip-172-16-30-3 ip-172-16-30-4
echo

echo ceph-deploy purgedata ip-172-16-30-2 ip-172-16-30-3 ip-172-16-30-4
ceph-deploy --username "pingcap" purgedata ip-172-16-30-2 ip-172-16-30-3 ip-172-16-30-4
echo

echo ceph-deploy forgetkeys
ceph-deploy --username "pingcap" forgetkeys

echo rm -rf ceph.*
rm -rf ceph.*
