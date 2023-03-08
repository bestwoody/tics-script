while :
do
  # loop infinitely
  itv=$(( ( RANDOM % 120 )  + 1 ))
  tiup bench rawsql -D tpch_0_1 -p $2 -U $1 -H  gateway01.us-east-1.dev.shared.aws.tidbcloud.com run --count 20 --query-files hehe.sql  --threads 2
  if [ $? -ne 0 ]; then
	  break
  fi
  echo ${itv}s
  sleep ${itv}s
done
