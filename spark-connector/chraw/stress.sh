for ((i=0;i<1000000;i++)); do
	./run.sh "select * from types" >> stress.log
done
