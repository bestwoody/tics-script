while :
do
	tiup bench rawsql -D tpch_0_1 -p $2 -U $1 -H  gateway01.us-east-1.$3.shared.aws.tidbcloud.com run --count 0 --query-files hehe.sql  --threads 4
done
