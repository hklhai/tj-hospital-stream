yarn-session.sh –n 2 -s 8 –jm 2048 –tm 10240 –qu root.default –nm tj -d


yarn application -list|grep tj|awk '{print $1}'
