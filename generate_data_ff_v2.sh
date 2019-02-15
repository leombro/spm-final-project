#!/bin/bash

if [ ! -d logs ]
then
	mkdir logs
fi
if [ ! -d logs/ff_v2 ]
then
	mkdir logs/ff_v2
fi
for j in {1..10}
do
	echo "repetition $j"
	for i in 0 1 2 4 8 16 32 64 128
	do
		echo "test $i ff_v2"
		if [ ! -d logs/ff_v2/$i ]
		then
			mkdir logs/ff_v2/$i
		fi
		./prefix_sum_ff_v2 1048576 $i 0 512 > logs/ff_v2/$i/test-$i-$j.log
	done
done
for i in 0 1 2 4 8 16 32 64 128
do
	echo "computing aggregates for $i threads"
	cat logs/ff_v2/$i/*.log | awk '{ print $14 "," $2 }' > logs/ff_v2/$i/aggregate.csv
done 
