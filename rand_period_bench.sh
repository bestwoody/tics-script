while :
do
  # loop infinitely
  itv=$(( ( RANDOM % 120 )  + 1 ))
  tiup bench rawsql run --count 20 --query-files hehe.sql  --db tpch_1 --threads 2 
  if [ $? -ne 0 ]; then
	  break
  fi
  echo ${itv}s
  sleep ${itv}s
done
