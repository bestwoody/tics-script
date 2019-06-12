verb="$1"

if [ -z "$verb" ] || [ "$verb" == "0" ]; then
	python simulator.py baseline conf/baseline.conf 0 | grep 'performance\|pattern'
else
	python simulator.py baseline conf/baseline.conf "$verb"
fi
