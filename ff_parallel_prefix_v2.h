//
// Created by Orlando Leombruni on 26/05/2018.
//

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
 * This version implements the work-efficient algorithm (see the project report) using FastFlow by means of
 * concatenation of two farm patterns:
 *
 * - An emitter splits the input array into Nw portions and assigns every portion to a phase-1 worker;
 * - Nw phase-1 workers compute the reduction of their own portion (using the oplus operator) in parallel;
 * - A middle node gathers the Nw intermediate results of phase-1 (an array of reduced values), computes
 *   a sequential exclusive scan on them, and then forwards an element of the intermediate array alongside a portion
 *   of the input array to phase-2 workers;
 * - Nw phase-2 workers perform a scan of their portion of the initial array using the provided element as the identity
 *   for the oplus operation;
 * - A final collector node moves the computed elements into a new array that will be returned to the user.
 *
 * Notice that, to fully model each node as an independent entity, each worker actually receives its portion of the
 * input array as a "new" array; this is in contrast with the C++11 thread implementation, where all workers
 * directly accessed the shared input array in a lock-free manner.
 *
 * @tparam T Type of elements of the input array.
 */
template <typename T>
class ff_parallel_prefix_v2 {

private:

    /**
     * A struct that represents a task to be computed.
     */
    struct Task {
        size_t taskID;                  // ID of the task
        size_t first;                   // Index of the first element of the portion in the input array
        size_t last;                    // Index of the first element of the next portion (i.e., last-1 is the "true"
                                        // last element of this portion), relative to the input array
        T initialValue;                 // Identity for the oplus operator for phase 1, or starting value for phase 2
        T reducedValue;                 // Result of the phase 1 on this portion
        std::vector<T> dataVec;         // Portion of the array to be computed
    };

    /**
     * The node that partitions the input array and assigns every partition to a different worker node.
     */
    struct Emitter: ff::ff_node {

        size_t pardeg;                      // Parallelism degree
        const T initial;                    // Identity for the oplus operator
        const std::function<T(T,T)> oplus;  // Operator of the parallel scan operation
        std::vector<T> data;                // The input array
        size_t sz;                          // Tentative size of each partition
        size_t rest;                        // Number of elements of the input array to be redistributed among workers
                                            // if parallelism degree is not a divisor of the input size

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
         * The service function of the emitter. Given input array size equal to N and parallelism degree equal to D, it
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
         * @param task  ignored as this node is the first of the pipeline em-workers-middle-workers-coll
         * @return      EOS when all tasks are assigned to phase-1 workers
         */
        void* svc(void* task) override {
            size_t prev = 0;
            size_t datasize = data.size();
            for (size_t i{0}; i < pardeg; ++i) {
                auto t = new Task();
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
                if (i == pardeg - 1) { // Just a precaution to avoid strange cases...
                    enditer = data.end();
                }
                // Elements are moved from the input array
                t->dataVec.insert(t->dataVec.end(),
                                  std::make_move_iterator(data.begin()),
                                  std::make_move_iterator(enditer));
                data.erase(data.begin(), enditer);
                ff_send_out(t);
            }
            return EOS;
        }
    };

    /**
     * The node that perform the reduction step over a portion of the array.
     */
    struct Phase1Worker: ff::ff_node {

        const std::function<T(T,T)> oplus;        // Operator to use in the reduction

        /**
         * Constructor for the phase 1 worker.
         *
         * @param f     The operator to use in the reduction
         */
        explicit Phase1Worker(const std::function<T(T,T)>& f): oplus(f) {
        }

        /**
         * The service function of a first phase worker, a simple sequential reduction.
         *
         * @param task  The task containing the array and the identity value
         * @return      The same task, with the result of the reduction in the "reducedValue" field
         */
        void* svc(void* task) override {
            auto t = static_cast<Task*>(task);
            T acc = t->initialValue;
            for (size_t i{0}; i < t->dataVec.size(); ++i) {
                acc = oplus(acc, t->dataVec.at(i));
            }
            t->reducedValue = acc;
            return t;
        }
    };

    /**
     * The "middle" node that acts as the collector for the first farm and the emitter for the second farm. It also
     * performs a sequential exclusive scan on the results of the first phase.
     *
     * In order to avoid having to destroy "first phase" tasks and create new "second phase" tasks, the former ones
     * are stored and reused for the second phase.
     */
    struct FirstCollector: ff::ff_node {

        size_t pardeg;                      // Parallelism degree
        const std::function<T(T, T)> oplus; // Operator for the sequential scan
        std::vector<T> reduction;           // Vector of results from the first phase
        std::vector<Task *> tasks;          // Vector to collect and store received tasks from first phase
        const T initial;                    // Identity value for the oplus operator
        size_t count{0};                    // Number of already-received tasks

        /**
         * Constructor for the middle node.
         *
         * @param _pardeg       the parallelism degree
         * @param _oplus        operator of the scan operation
         * @param _identity     reference to the identity for the oplus operator
         */
        FirstCollector(size_t _pardeg, const std::function<T(T, T)>& _oplus, const T& _identity) :
                pardeg(_pardeg),
                oplus(_oplus),
                initial(_identity) {
            // Ensure that both the reduction and tasks arrays contain exactly pardeg elements
            reduction.resize(pardeg);
            tasks.resize(pardeg);
        }

        /**
         * Service function for the middle node.
         *
         * @param task      task containing the intermediate result computed by a phase 1 worker
         * @return          GO_ON if there are some workers that still have to deliver their result; the last
         *                  task, otherwise (all other tasks are delivered via ff_send_out)
         */
        void *svc(void *task) override {
            auto t = static_cast<Task *>(task);
            reduction.at(t->taskID) = t->reducedValue;
            count++;
            tasks.at(t->taskID) = t; // Store the task to reuse it
            if (count == pardeg) {
                T acc = initial;
                // Sequential exclusive scan
                for (size_t i{0}; i < pardeg - 1; ++i) {
                    Task *out = tasks.at(i);
                    T temp = reduction.at(i);
                    out->initialValue = acc;
                    acc = oplus(acc, temp);
                    ff_send_out(out);
                }
                Task* out = tasks.at(pardeg - 1);
                out->initialValue = acc;
                return out;
            } else {
                return GO_ON;
            }
        }
    };

    /**
     * Node that performs a sequential scan over its portion of the array and delivers the result to the final
     * collector.
     */
    struct Phase2Worker: ff::ff_node {

        const std::function<T(T,T)> oplus;        // Operator for the scan operation
        bool inclusive;                           // Whether the user has requested an inclusive or exclusive scan

        /**
         * Constructor for the phase 2 worker.
         *
         * @param _incl     whether the scan should be inclusive or exclusive
         * @param _oplus    the operator for the scan
         */
        Phase2Worker(bool _incl, const std::function<T(T,T)>& _oplus):
                inclusive(_incl),
                oplus(_oplus) {
        }

        /**
         * Service function of the phase 2 worker. It simply performs a sequential scan over its portion
         * of the array, inclusive or exclusive depending on the user choice.
         *
         * @param task      task containing the array portion and the initial value for the scan
         * @return          the same task, this time containing the array result of the scan
         */
        void* svc(void* task) override {
            auto t = static_cast<Task*>(task);
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
            return t;
        }
    };

    /**
     *  Node that collects results from the second phase and puts them into the array to be returned to the user.
     */
    struct FinalCollector: ff::ff_node {

        std::vector<T> results;     // array to be returned to the user
        size_t sz;                  // size of the array (same as the input array)

        /**
         * Constructor for the collector node.
         *
         * @param size      size of the result array
         */
        explicit FinalCollector(size_t size):
                sz(size) {
            results.resize(sz);
        }

        /**
         * Service function for the collector node. It simply extracts the partitions from the results received from
         * the second farm and puts them (in the right place) in the results array.
         *
         * @param task      task containing a result from the second phase
         * @return          GO_ON to receive the next result
         */
        void* svc(void* task) override {
            auto t = static_cast<Task*>(task);
            std::move(std::make_move_iterator(t->dataVec.begin()),
                      std::make_move_iterator(t->dataVec.end()),
                      results.begin() + t->first);
            delete t;
            return GO_ON;
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
    explicit ff_parallel_prefix_v2(bool _inclusive):
            inclusive(_inclusive) {
    }

    /**
     * A constructor to specify both the parallelism degree and the type of desired scan. Other parameters must be
     * provided at a later time.
     *
     * @param _inclusive        whether the scan should be inclusive or exclusive
     * @param _pardegree        the desired parallelism degree
     */
    ff_parallel_prefix_v2(bool _inclusive, int _pardegree):
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
    ff_parallel_prefix_v2(std::vector<T> _data, std::function<T(T,T)> _oplus, T _identity, bool _inclusive):
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
    ff_parallel_prefix_v2(std::vector<T> _data,
                       std::function<T(T,T)> _oplus,
                       T _identity,
                       bool _inclusive,
                       int _pardegree):
            data(std::move(_data)),
            oplus(std::move(_oplus)),
            initial(std::move(_identity)),
            inclusive(_inclusive),
            pardegree(_pardegree) {
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
        if (setFun && setData) setUp = true; // All required parameters have been provided
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
     *  If computing it in parallel, it builds a FastFlow pipeline composed of two farm stages, where the collector of
     *  the first farm is also the emitter of the second farm (the "middle" stage as described above).
     */
    void compute() {
        if (setUp) {
            if (pardegree == 0) sequentialScan();
            else {
                std::vector<ff::ff_node *> workersPhase1;
                std::vector<ff::ff_node *> workersPhase2;
                for (size_t i{0}; i < pardegree; ++i) {
                    // No std::make_unique, for full compatibility with C++11
                    workersPhase1.push_back(new Phase1Worker(oplus));
                    workersPhase2.push_back(new Phase2Worker(inclusive, oplus));
                }
                auto nthr = static_cast<size_t>(pardegree); // Allowed and correct since we assured that pardegree > 0
                size_t size = data.size();
                auto e = new Emitter(nthr, initial, oplus, data);
                auto first = new FirstCollector(nthr, oplus, initial);
                auto final = new FinalCollector(size);
                auto farm1 = new ff::ff_farm<>(workersPhase1, e);
                farm1->remove_collector();
                auto farm2 = new ff::ff_farm<>(workersPhase2, first, final);
                farm2->setMultiInput();
                ff::ff_pipeline p;
                p.add_stage(farm1);
                p.add_stage(farm2);
                if (p.run_and_wait_end() < 0) throw std::runtime_error("in executing pipe");
                data = std::move(final->results);
                for (size_t i{0}; i < pardegree; ++i) {
                    delete workersPhase1.at(i);
                    delete workersPhase2.at(i);
                }
                delete e;
                delete first;
                delete final;
                delete farm1;
                delete farm2;
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
