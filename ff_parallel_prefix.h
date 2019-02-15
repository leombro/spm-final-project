#ifndef SPM_FINAL_PROJECT_FF_PARALLEL_PREFIX_H
#define SPM_FINAL_PROJECT_FF_PARALLEL_PREFIX_H

#include <utility>
#include <vector>
#include <functional>
#include <exception>
#include <algorithm>
#include <iostream>
#include <chrono>
#include <ff/node.hpp>
#include <ff/farm.hpp>
#include <ff/utils.hpp>
#include <ff/pipeline.hpp>

/**
 * Class to compute the "parallel prefix" operation over an array of elements of arbitrary type using an
 * associative and commutative operator oplus.
 *
 * This version implements the work-efficient algorithm (see the project report) using FastFlow by means of a single
 * farm that wraps around:
 *
 * - An emitter splits the input array into Nw portions and assigns every portion to a worker;
 * - Nw workers compute the reduction of their own portion (using the oplus operator) in parallel, then they
 *   send the results to the collector;
 * - The collector node gathers the Nw intermediate results of phase 1 (an array of reduced values), computes
 *   a sequential exclusive scan on them, and then forwards an element of the intermediate array alongside a portion
 *   of the input array back to the emitter
 * - At the beginning of the second phase, the emitter forwards any task "as-is" to the workers;
 * - The workers now perform a scan of their portion of the initial array using the provided element as the identity
 *   for the oplus operation, then send the results to the collector
 * - The collector finally moves the computed elements into a new array that will be returned to the user.
 *
 * Notice that, to fully model each node as an independent entity, each worker actually receives its portion of the
 * input array as a "new" array; this is in contrast with the C++11 thread implementation, where all workers
 * directly accessed the shared input array in a lock-free manner.
 *
 * @tparam T Type of elements of the input array.
 */
template <typename T>
class ff_parallel_prefix {

private:

    /**
     * A struct that represents a task to be computed.
     */
    struct Task {

        /**
         * Enum to differentiate between a first phase task and second phase task.
         */
        enum Type {
            FIRST_PHASE,
            SECOND_PHASE
        };

        Task::Type type;                // First or second phase task
        size_t taskID;                  // ID of the task
        size_t first;                   // Index of the first element of the portion in the input array
        size_t last;                    // Index of the first element of the next portion (i.e., last-1 is the "true"
                                        // last element of this portion), relative to the input array
        T initialValue;                 // Identity for the oplus operator for phase 1, or starting value for phase 2
        T reducedValue;                 // Result of the phase 1 on this portion
        std::vector<T> dataVec;         // Portion of the array to be computed
    };

    /**
     * The node that:
     *
     * - at the beginning, partitions the input array and assigns every partition to a different worker node, starting
     *   phase 1;
     * - after that, it receives "wrapped-around" tasks from the collector and immediately forwards them to workers.
     */
    struct Emitter: ff::ff_node {

        size_t pardeg;                      // Parallelism degree
        const T initial;                    // Identity for the oplus operator
        const std::function<T(T,T)> oplus;  // Operator of the parallel scan operation
        std::vector<T> data;                // The input array
        size_t sz;                          // Tentative size of each partition
        size_t rest;                        // Number of elements of the input array to be redistributed among workers
                                            // if parallelism degree is not a divisor of the input size
        int count{0};                       // Number of already-received tasks from the collector (phase 2)

        /**
         * Constructor for the Emitter class.
         *
         * @param _pardeg       the parallelism degree
         * @param _identity     identity for the oplus operator
         * @param _oplus        operator of the scan operation
         * @param _input        input array
         */
        Emitter(size_t _pardeg, const T& _identity, const std::function<T(T,T)>& _oplus, std::vector<T>& _input):
                pardeg(_pardeg),
                initial(_identity),
                oplus(_oplus),
                data(std::move(_input)) {
            sz = data.size() / pardeg;
            rest = data.size() % pardeg;
        }

        /**
         * The service function of the emitter.
         *
         * In the first phase, given input array size equal to N and parallelism degree equal to D, it
         * splits the input array in the following manner:
         *
         * - if N % D = 0, each portion will have exactly N / D elements
         * - if N % D = K != 0, the first K portions will have (N / D) + 1 elements, the other N - K will have N / D
         *
         * This is done to achieve a better balance of elements in each worker in situations where N % D >> N / D,
         * where a simple naïve partition (N / D elements for D - 1 workers, N / D + N % D for the D-th one) would cause
         * an excessive load on the last worker (e.g. an input array of 20 elements with parallelism degree 8 would see
         * the first 7 worker receive 2 elements each and the last worker receive 6 elements with the naïve partition,
         * while with this solution we will have 4 workers with 3 elements and 4 workers with 2 elements).
         *
         * In the second phase, it simply forwards to the workers the tasks it received from the collector.
         *
         * @param task  either nullptr in the first phase, or a task to forward to a worker in the second phase
         * @return      GO_ON during the first phase, the received task in the second phase, or EOS when the computation
         *              ends
         */
        void* svc(void* task) override {
            if (task == nullptr) {
                size_t prev = 0;
                size_t datasize = data.size();
                for (size_t i{0}; i < pardeg; ++i) {
                    auto t = new Task();
                    t->type = Task::Type::FIRST_PHASE;
                    t->taskID = i;
                    t->first = prev;
                    size_t last = (i == pardeg - 1) ? datasize : prev + sz;
                    int plus = (rest > 0) ? 1 : 0;
                    last = last + plus;
                    if (rest > 0) rest--;
                    prev = last;
                    t->last = last;
                    t->initialValue = initial;
                    t->reducedValue = initial;
                    auto enditer = data.begin() + sz + plus;
                    if (i == pardeg - 1) {
                        enditer = data.end();
                    }
                    t->dataVec.insert(t->dataVec.end(),
                                      std::make_move_iterator(data.begin()),
                                      std::make_move_iterator(enditer));
                    data.erase(data.begin(), enditer);
                    ff_send_out(t);
                }
                return GO_ON; // Worker must be alive until the second phase
            } else {
                auto t = static_cast<Task*>(task);
                t->type = Task::Type::SECOND_PHASE; // formally redefine tasks as belonging to the second phase
                count++;
                if (count < pardeg) {
                    return t;
                } else {
                    // After sending the last element, send EOS
                    ff_send_out(t);
                    return EOS;
                }
            }
        }

    };

    /**
     * A worker node, that performs a reduction (phase 1) or a scan (phase 2) of its portion of the input.
     */
    struct Worker: ff::ff_node {

        const std::function<T(T,T)> oplus;      // Operator to use in the reduction/scan
        bool inclusive;                         // Whether the user has requested an inclusive or exclusive scan

        /**
         * Constructor for the worker nodes.
         *
         * @param _incl     whether the scan should be inclusive or exclusive
         * @param _oplus    the operator for the scan
         */
        Worker(bool _incl, const std::function<T(T,T)>& _oplus):
                oplus(_oplus),
                inclusive(_incl) {
        }

        /**
         * The service function of a worker.
         *
         * During phase 1, the worker computes the reduce operation over its portion of the array and sends the result
         * to the collector.
         *
         * During phase 2, it performs a sequential scan over its portion of the array, inclusive or exclusive depending
         * on the user choice, and then again sends the result to the collector.
         *
         * @param task  The task, tagged either with phase 1 or phase 2 label
         * @return      The same task, with the result of computed operation
         */
        void* svc(void* task) override {
            auto t = static_cast<Task*>(task);
            switch (t->type) {
                case Task::Type::FIRST_PHASE: {
                    // First phase: reduction
                    T acc = t->initialValue;
                    for (size_t i{0}; i < t->dataVec.size(); ++i) {
                        acc = oplus(acc, t->dataVec.at(i));
                    }
                    t->reducedValue = acc;
                    break;
                }
                case Task::Type::SECOND_PHASE: {
                    // Second phase: scan
                    if (inclusive) {
                        for (size_t i{0}; i < t->dataVec.size(); ++i) {
                            t->initialValue = oplus(t->initialValue, t->dataVec.at(i));
                            t->dataVec.at(i) = t->initialValue;
                        }
                    } else {
                        if (t->dataVec.size() > 0) {
                            for (size_t i{0}; i < t->dataVec.size() - 1; ++i) {
                                T temp = t->dataVec.at(i);
                                t->dataVec.at(i) = t->initialValue;
                                t->initialValue = oplus(t->initialValue, temp);
                            }
                            t->dataVec.at(t->last - 1) = t->initialValue;
                        }
                    }
                }
            }
            return t;
        }

    };

    /**
     *  Node that collects results from the workers, and
     *
     *  - in the first phase, it waits until all workers deliver their result, then performs a serial exclusive scan
     *    over the computed reduced values, and finally sends back the tasks to the emitter
     *  - in the second phase, collects results and puts them into the array to be returned to the user.
     */
    struct Collector: ff::ff_node {

        size_t pardeg;                          // Parallelism degree
        const std::function<T(T, T)> oplus;     // Operator for the sequential scan
        std::vector<T> reduction;               // Vector of results from the first phase
        std::vector<Task *> tasks;              // Vector to collect and store received tasks from first phase
        const T initial;                        // Identity value for the oplus operator
        size_t count{0};                        // Number of already-received tasks
        std::vector<T> results;                 // array to be returned to the user
        size_t sz;                              // size of the array (same as the input array)

        /**
         * Constructor for the collector node.
         *
         * @param _pardeg       the parallelism degree
         * @param _oplus        operator of the scan operation
         * @param _identity     reference to the identity for the oplus operator
         * @param _size         size of the result array
         */
        Collector(size_t _pardeg, const std::function<T(T, T)>& _oplus, const T& _identity, size_t _size) :
                pardeg(_pardeg),
                oplus(_oplus),
                initial(_identity),
                sz(_size) {
            // Ensure that both the reduction and tasks arrays contain exactly pardeg elements, and that the result
            // array contains exactly the same number of items as the input array
            reduction.resize(pardeg);
            tasks.resize(pardeg);
            results.resize(sz);
        }

        /**
         * Service function for the collector node.
         *
         * In the first phase, it stores received tasks until all workers finish their job, and then first computes the
         * exclusive scan over the reduced values and then send all the tasks back to the emitter.
         *
         * In the second phase, it simply stores the partitions from the results received from the workers and puts them
         * (in the right place) in the results array.
         *
         * @param task      task received by the workers
         * @return          During phase 1, GO_ON if there are some workers that still have to deliver their result, or
         *                  the last task otherwise (all other tasks are delivered via ff_send_out).
         *                  During phase 2, GO_ON to receive the next result
         */
        void *svc(void *task) override {
            auto t = static_cast<Task *>(task);
            switch (t->type) {
                case Task::Type::FIRST_PHASE: {
                    // Phase 1: "Barrier" + sequential exclusive scan + send back to emitter
                    reduction.at(t->taskID) = t->reducedValue;
                    count++;
                    tasks.at(t->taskID) = t;
                    if (count == pardeg) {
                        T acc = initial;
                        for (size_t i{0}; i < pardeg - 1; ++i) {
                            Task *out = tasks.at(i);
                            T temp = reduction.at(i);
                            out->initialValue = acc;
                            acc = oplus(acc, temp);
                            ff_send_out(out);
                        }
                        Task *out = tasks.at(pardeg - 1);
                        out->initialValue = acc;
                        return out;
                    } else {
                        return GO_ON;
                    }
                }
                case Task::Type::SECOND_PHASE: {
                    // Phase 2: place results in final array
                    std::move(std::make_move_iterator(t->dataVec.begin()),
                              std::make_move_iterator(t->dataVec.end()),
                              results.begin() + t->first);
                    delete t;
                    return GO_ON;
                }
            }
        }

    };

    std::vector<T> data;                // the input array on which to perform the scan operation
    bool setData{false};                // whether the input array has been provided
    std::function<T(T,T)> oplus;        // operator for the scan
    T initial;                          // identity value for the oplus operator
    bool setFun{false};                 // whether the operator and identity have been provided
    int pardegree{-1};                  // desired parallelism degree: 0 for "pure" sequential, -1 if not set
    bool inclusive;                     // whether the scan should be inclusive or exclusive
    bool setUp{false};                  // whether all needed parameters have been provided

    /**
     * Pure sequential scan, to be performed in case the user requests so (pardegree = 0).
     */
    void sequentialScan() {
        if (setFun && setData) {
            if (inclusive) {
                for (size_t i{0}; i < data.size(); ++i) {
                    initial = oplus(initial, data.at(i));
                    data.at(i) = initial;
                }
            } else {
                size_t sz = data.size();
                if (sz > 0) {
                    for (size_t i{0}; i < sz - 1; ++i) {
                        T temp = data.at(i);
                        data.at(i) = initial;
                        initial = oplus(initial, temp);
                    }
                    data.at(sz - 1) = initial;
                }
            }
        }
    }

public:

    /**
     * Basic constructor for the class. The user only states whether the scan should be exclusive or inclusive,
     * and leaves the definition of other parameters for a later time.
     *
     * @param _inclusive        whether the scan should be inclusive or exclusive
     */
    explicit ff_parallel_prefix(bool _inclusive):
            inclusive(_inclusive) {
    }

    /**
     * A constructor to specify both the parallelism degree and the type of desired scan. Other parameters must be
     * provided at a later time.
     *
     * @param _inclusive        whether the scan should be inclusive or exclusive
     * @param _pardegree        the desired parallelism degree
     */
    ff_parallel_prefix(bool _inclusive, int _pardegree):
            inclusive(_inclusive),
            pardegree(_pardegree) {
    }

    /**
     * A constructor to specify all parameters except the parallelism degree, that must be provided at a later time.
     *
     * @param _data             input array
     * @param _oplus            operator for the scan
     * @param _identity         identity value for the oplus operator
     * @param _inclusive        whether the scan should be inclusive or exclusive
     */
    ff_parallel_prefix(std::vector<T> _data, std::function<T(T,T)> _oplus, T _identity, bool _inclusive):
            data(std::move(_data)),
            oplus(std::move(_oplus)),
            initial(std::move(_identity)),
            inclusive(_inclusive),
            setData(true),
            setFun(true) {
    }

    /**
     * A constructor to specify all parameters. If all parameters are correct (in particular, a non-negative parallelism
     * degree is required), it leaves the object in the "ready to compute" state, unlike other constructors.
     *
     * @param _data             input array
     * @param _oplus            operator for the scan
     * @param _identity         identity value for the oplus operator
     * @param _inclusive        whether the scan should be inclusive or exclusive
     * @param _pardegree        the desired parallelism degree
     */
    ff_parallel_prefix(std::vector<T> _data,
                          std::function<T(T,T)> _oplus,
                          T _identity,
                          bool _inclusive,
                          int pardegree):
            data(std::move(_data)),
            oplus(std::move(_oplus)),
            initial(std::move(_identity)),
            inclusive(_inclusive),
            pardegree(pardegree) {
        if (pardegree > -1) setUp = true;
    }

    /**
     * Sets the parallelism degree.
     *
     * @param _pardegree        the desired parallelism degree (valid if > -1)
     */
    void setParallelism(int _pardegree) {
        if (_pardegree < 0) return;
        pardegree = _pardegree;
        if (setFun && setData) setUp = true;    // All required parameters have been provided
    }

    /**
     * Sets the input data array.
     *
     * @param _data             the input data array
     */
    void setInputVector(std::vector<T> _data) {
        data = std::move(_data);
        setData = true;
        if (setFun && pardegree >= 0) setUp = true; // All required parameters have been provided
    }

    /**
    * Sets the operator and its relative identity value.
    *
    * @param _oplus            operator for the scan
    * @param _identity         identity value for the oplus operator
    */
    void setOperator(std::function<T(T,T)> _oplus, T _identity) {
        oplus = std::move(_oplus);
        initial = std::move(_identity);
        setFun = true;
        if (setData && pardegree >= 0) setUp = true; // All required parameters have been provided
    }

    /**
     *  Computes the scan operation, either in parallel (pardegree > 0) or sequentially (pardegree = 0).
     *
     *  If computing it in parallel, it builds a FastFlow farm with wrap-around using the structs defined above.
     */
    void compute() {
        if (pardegree == 0) sequentialScan();
        else {
            if (setUp) {
                std::vector<ff::ff_node *> workersPhase1;
                for (size_t i{0}; i < pardegree; ++i) {
                    // No std::make_unique, for full compatibility with C++11
                    workersPhase1.push_back(new Worker(inclusive, oplus));
                }
                auto nthr = static_cast<size_t>(pardegree);
                size_t size = data.size();
                auto e = new Emitter(nthr, initial, oplus, data);
                auto first = new Collector(nthr, oplus, initial, size);
                auto farm = new ff::ff_farm<>(workersPhase1, e, first);
                farm->wrap_around();
                if (farm->run_and_wait_end() < 0) throw std::runtime_error("in executing pipe");
                data = std::move(first->results);
                for (size_t i{0}; i < pardegree; ++i) {
                    delete workersPhase1.at(i);
                }
                delete e;
                delete first;
                delete farm;
            }
        }
    }

    /**
     * Extracts the current data. If compute() has been called, this is the result of the scan operation.
     *
     * @return          the current data array.
     */
    std::vector<T> popData() {
        setData = false;
        setUp = false;
        return std::move(data);
    }

};

#endif //SPM_FINAL_PROJECT_FF_PARALLEL_PREFIX_H
