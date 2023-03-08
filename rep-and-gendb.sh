sed -e "s/HHUSER/$1/g" integrated/_base/test/load_data.sh.template > integrated/_base/test/load_data.sh.t1
sed -e "s/HHPWD/$2/g" integrated/_base/test/load_data.sh.t1 > integrated/_base/test/load_data.sh
pushd ./integrated/tools && bash tpch_load.sh gateway01.us-east-1.$3.shared.aws.tidbcloud.com 4000 0.1 all
popd
