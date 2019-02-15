
#include <iostream>
#include <sstream>
#include <random>
#include <chrono>
#include "ff_parallel_prefix.h"

/**
 * This sample application computes the prefix sum over an array of randomly-generated numbers.
 * Optionally, one can also indicate a seed (to generate predictable numbers) and a number of
 * "waste iterations" to artificially increase the computation time. To specify the latter,
 * also the seed must be specified
 *
 * @param argc      number of arguments (at least 2: data size and parallelism degree)
 * @param argv      arguments
 * @return          0 if computation is successful, 1 otherwise
 */

int main(int argc, char* argv[]) {

    static int max_print_size = 32768;       // Totally arbitrary limit on input size for printing (i.e. having a
                                             // data size of more than max_print_size will not print the result array
                                             // on stdout

    int pardegree;
    static int waste_iterations;
    int data_size;
    unsigned int seed;
    std::vector<long long> data;


    if (argc < 3) {
        std::cout << "Must specify at least input data size and parallelism degree\n"
                  << "Optional parameters: RNG seed, waste iterations per operation\n"
                  << "Usage: " << argv[0] << " data_size par_degree [seed] [waste]" << std::endl;
        return 1;
    }

    std::istringstream data_iss{argv[1]}, pardeg_iss{argv[2]};

    if (!(data_iss >> data_size) || data_size < 1) {
        std::cout << "Invalid size of data\n"
                  << "Usage: " << argv[0] << " data_size par_degree [seed] [waste]" << std::endl;
        return 1;
    }

    if (!(pardeg_iss >> pardegree) || pardegree < 0) {
        std::cout << "Invalid parallelism degree\n"
                  << "Usage: " << argv[0] << " data_size par_degree [seed] [waste]" << std::endl;
        return 1;
    }

    if (argc > 3) {

        std::istringstream seed_iss{argv[3]};
        if (!(seed_iss >> seed)) {
            std::cout << "Invalid seed\n"
                      << "Usage: " << argv[0] << " data_size par_degree [seed] [waste]" << std::endl;
            return 1;
        }

        if (argc > 4) {
            std::istringstream waste_iss{argv[4]};
            if (!(waste_iss >> waste_iterations) || waste_iterations < 0) {
                std::cout << "Invalid number of waste iterations\n"
                          << "Usage: " << argv[0] << " data_size par_degree [seed] [waste]" << std::endl;
                return 1;
            }
        } else {
            waste_iterations = 0;
        }

    } else {
        std::random_device rand{};
        seed = rand();
        waste_iterations = 0;
    }

    // Creating the input data
    std::mt19937 random_generator(seed);
    std::uniform_int_distribution<> distribution(1, data_size * 10);

    for (int i{0}; i < data_size; ++i) {
        data.push_back(distribution(random_generator));
    }

    // The operator for the prefix sum, i.e. an addition operator with the possibility to waste time
    // The identity of the operator is, of course, the number 0

    auto myOplus = [](long long a, long long b) {
        double x = 0;
        if (waste_iterations > 0) {
            x = a;
            for(int i{0}; i < (waste_iterations + 50); ++i) { // the "+50" is to ensure that x is always zero at the end
                x = sin(x);
            }
        }
        return a + b + static_cast<long long>(x);
    };

    // End of the creation of the required parameters/data. Start the clock measurement now
    auto start_time = std::chrono::high_resolution_clock::now();

    // Creation of the parallel prefix object
    ff_parallel_prefix<long long> prefix(true);        // Prefix sum is an inclusive parallel scan

    // Set the parallelism degree
    prefix.setParallelism(pardegree);

    // Set the operator and relative identity
    prefix.setOperator(myOplus, 0);

    // Set the input data
    prefix.setInputVector(data);

    // Start the actual computation
    prefix.compute();

    // Receive the result
    data = prefix.popData();

    // End of operations; stop the clock measurement
    auto end_time = std::chrono::high_resolution_clock::now();
    auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

    // Print the result on stdout on a single line, if not too big (useful for diff)
    if (data_size <= max_print_size) {
        for (const auto &el: data) {
            std::cout << el << " ";
        }
        std::cout << std::endl;
    }

    // Print the elapsed time
    std::cout << "Spent " << time_elapsed.count() << " us computing prefix sum of an array of size " << data_size
              << " using " << pardegree << " threads" << std::endl;

    return 0;
}