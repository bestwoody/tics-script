set -eu

cores=`cat /proc/cpuinfo | grep 'core id' | wc -l`
model=`cat /proc/cpuinfo | grep 'model name' | head -n 1 | awk -F ': ' '{print $2}'`
cpu_hz=`cat /proc/cpuinfo | grep 'cpu MHz' | head -n 1 | awk -F ': ' '{print $2}'`

mem=`free -h | grep Mem | awk '{print $2}'`
mem_hz=`sudo dmidecode -t memory | grep -i Speed | awk '{print $2}'`
if [ ! -z "$mem_hz" ]; then
	mem_hz="@ $mem_hz MHz"
fi

echo "### Hardware"
echo "* CPU: $cores Cores, $model @ $cpu_hz MHz"
echo "* Memory: ${mem}${mem_hz}"
echo
echo "### Software"
echo "* `rpm -q centos-release`"