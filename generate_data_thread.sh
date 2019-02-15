#!/bin/bash

if [ ! -d logs ]
then
	mkdir logs
fi
if [ ! -d logs/threads]
then
	mkdir logs/threads
fi
for j in {1..10}
do
	echo "repetition $j"
	for i in 0 1 2 4 8 16 32 64 128 256
	do
		echo "test $i thread"
		if [ ! -d logs/threads/$i ]
		then
			mkdir logs/threads/$i
		fi
		./prefix_sum 1048576 $i 0 512 > logs/threads/$i/test-$i-$j.log
	done
done
for i in 0 1 2 4 8 16 32 64 128 256
do
	echo "computing aggregates for $i threads"
	cat logs/threads/$i/*.log | awk '{ print $14 "," $2 }' > logs/threads/$i/aggregate.csv
done 
