#!/bin/bash

if [ ! -d logs ]
then
	mkdir logs
fi
if [ ! -d logs/ff ]
then
	mkdir logs/ff
fi
for j in {1..10}
do
	echo "repetition $j"
	for i in 0 1 2 4 8 16 32 64 128 256
	do
		echo "test $i ff"
		if [ ! -d logs/ff/$i ]
		then
			mkdir logs/ff/$i
		fi
		./prefix_sum_ff 1048576 $i 0 512 > logs/ff/$i/test-$i-$j.log
	done
done
for i in 0 1 2 4 8 16 32 64 128 256
do
	echo "computing aggregates for $i threads"
	cat logs/ff/$i/*.log | awk '{ print $14 "," $2 }' > logs/ff/$i/aggregate.csv
done 
