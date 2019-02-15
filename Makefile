FASTFOLDER=/usr/local/fastflow
FLAGS=-pthread -O3

.PHONY: clean thread ff ffv2 dataplot_thread dataplot_ff dataplot_ff_v2 test

thread: prefix_sum.cpp ParallelPrefix.h
	g++ -o prefix_sum $(FLAGS) $^

ff: prefix_sum_ff.cpp ff_parallel_prefix.h
	g++ -o prefix_sum_ff $(FLAGS) -I$(FASTFOLDER) $^

ffv2: prefix_sum_ff_v2.cpp ff_parallel_prefix_v2.h
	g++ -o prefix_sum_ff_v2 $(FLAGS) -I$(FASTFOLDER) $^

dataplot_thread: thread generate_data_thread.sh
	./generate_data_thread.sh

dataplot_ff: ff generate_data_ff.sh
	./generate_data_ff.sh

dataplot_ff_v2: thread generate_data_ff_v2.sh
	./generate_data_ff_v2.sh

test: thread ff ffv2 correctness_test.sh test_result
	./correctness_test.sh

clean:
	rm -f prefix_sum prefix_sum_ff prefix_sum_ff_v2