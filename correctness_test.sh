#!/bin/bash

for i in prefix_sum prefix_sum_ff prefix_sum_ff_v2
do
	./$i 1024 16 0 > temp.log
	if ! diff test_result <(head -n 1 temp.log)
	then
		echo "$i produced different output"
		rm -f temp.log
		exit 1
	fi
	rm -f temp.log
done
echo "tests ok"