//
// Created by Orlando Leombruni on 22/05/2018.
//

#ifndef SPM_FINAL_PROJECT_PARALLELPREFIX_H
#define SPM_FINAL_PROJECT_PARALLELPREFIX_H

#include <vector>
#include <thread>
#include <functional>
#include <deque>
#include <mutex>
#include <chrono>
#include <condition_variable>

/**
 * A class to implement a single-use barrier.
 */
class SingleBarrier {

private:

    std::mutex mutex;               // Mutex of the barrier
    std::condition_variable cv;     // Condition variable where the threads will wait
    std::size_t count;              // Number of threads in the barrier

public:

    /**
     * Sets the number of threads in the barrier.
     * @param n_threads the number of threads that will wait in the barrier
     */
    void setCount(size_t n_threads) {
        count = n_threads;
    }

    /**
     * Waits until all other threads reach the barrier.
     */
    void wait() {
        std::unique_lock<std::mutex> lock{mutex};
        if (--count == 0) {
            cv.notify_all();
        } else {
            cv.wait(lock, [this] { return count == 0; });
        }
    }
};

/**
 * Class to compute the "parallel prefix" operation over an array of elements of arbitrary type using an
 * associative and commutative operator oplus.
 *
 * This version implements the work-efficient algorithm (see the project report) using C++11 Threads by means of a
 * single farm that wraps around:
 *
 * - A central coordinator, the main thread, splits the input into Nw portions and assigns every portion to a worker;
 * - Nw workers compute the reduction of their own portion (using the oplus operator) in parallel, then they
 *   send the results back to the coordinator;
 * - The coordinator gathers the Nw intermediate results of phase 1 (an array of reduced values), computes
 *   a sequential exclusive scan on them, and then forwards an element of the intermediate array alongside a portion
 *   of the input array back to a worker, starting the second phase;
 * - The workers now perform a scan of their portion of the initial array using the provided element as the identity
 *   for the oplus operation, then send the results to the coordinator;
 * - The coordinator finally moves the computed elements into a new array that will be returned to the user.
 *
 * Notice that in this implementation the shared memory aspect of C++11 threads is fully utilized to avoid having to
 * move pieces of the input array around; each worker will receive simply the indices of the bound of its own portion,
 * and will access the shared data structure without locks (since the portions are partitions of the whole array).
 *
 * @tparam T Type of elements of the input array.
 */
template <typename T>
class ParallelPrefix {

private:

    /**
     * Struct to represent a task to be computed by a worker.
     */
    struct Task {

        /**
         * Enum to differentiate between first and second phase tasks.
         */
        enum Type {
            ReduceStep,         // Task for the first phase
            ExclusiveScan,      // Task for the second phase, when the user has requested an exclusive scan
            InclusiveScan,      // Task for the second phase, when the user has requested an inclusive scan
            Stop                // Signals that the computation has ended and the thread can terminate
        };

        Task::Type type;        // Type of the task
        size_t first;           // Index of the first element of the portion in the input array
        size_t last;            // Index of the first element of the next portion (i.e., last-1 is the "true"
                                // last element of this portion), relative to the input array
        T initialValue;         // Identity for the oplus operator for phase 1, or starting value for phase 2
    };

    /**
     * A specialized synchronized queue to act as a channel between the coordinator and a single worker.
     */
    class TaskChannel {

    private:

        std::deque<ParallelPrefix::Task> deque;     // The underlying data structure
        std::mutex m;                               // Mutex for protecting concurrent access
        std::condition_variable cond;               // Condition variable

    public:

        /**
         * Pushes a new task into the channel.
         *
         * @param val The task pushed into the channel
         */
        void push(const Task& val) {
            std::unique_lock<std::mutex> lock(m);
            deque.push_front(val);
            cond.notify_one();
            lock.unlock();
        }

        /**
         * Retrieves a task from the channel, blocking if the channel is empty.
         *
         * @return a task
         */
        Task pop() {
            std::unique_lock<std::mutex> lock(m);
            cond.wait(lock, [=]{ return !deque.empty(); });
            Task ret(std::move(deque.back()));
            deque.pop_back();
            return ret;
        }
    };

    std::vector<T> data;                    // the input array on which to perform the scan operation
    bool setData{false};                    // whether the input array has been provided
    std::function<T(T,T)> oplus;            // operator for the scan
    T initial;                              // identity value for the oplus operator
    bool setFun{false};                     // whether the operator and identity have been provided
    int pardegree{-1};                      // desired parallelism degree: 0 for "pure" sequential, -1 if not set
    bool inclusive;                         // whether the scan should be inclusive or exclusive
    bool setUp{false};                      // whether all needed parameters have been provided

    std::vector<T> partialResults;          // Vector of results from the first phase

    std::vector<TaskChannel*> channels;     // Vector of channels between coordinator and workers
    std::vector<std::thread> threadPool;    // Vector of worker threads
    SingleBarrier b;                        // Instance of the SingleBarrier defined above

    /**
     * Function to stop the computations: sends an end-of-stream message to each channel, and waits for every thread
     * to join.
     */
    void stop() {
        if (pardegree < 0) return; // Failsafe if called when not properly set-up
        for (size_t i{0}; i < pardegree; ++i) {
            Task t;
            t.type = Task::Type::Stop;
            channels.at(i)->push(t);
        }
        for (size_t i{0}; i < pardegree; ++i) {
            threadPool.at(i).join();
            delete channels.at(i);
        }
    }

    /**
     * Initializes the barrier object, the thread pool and the channel set.
     */
    void setupThreadPool() {
        b.setCount(static_cast<size_t>(pardegree+1)); // (pardegree + 1) because also the coordinator waits
        for (size_t i{0}; i < pardegree; ++i) {
            auto t = new TaskChannel();
            partialResults.push_back(initial);
            channels.push_back(t);
            threadPool.emplace_back(&ParallelPrefix::Worker, this, i);
        }
    }

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

    /**
     * Service function of the worker threads.
     *
     * During phase 1, the worker computes the reduce operation over its portion of the array and writes the result
     * in the shared array, then waits at the barrier.
     *
     * During phase 2, it performs a sequential scan over its portion of the array, inclusive or exclusive depending
     * on the user choice, and then again writes the result in the shared array
     *
     * @param workerID ID of the worker, used for selecting the right channel and placing the intermediate result in
     *                 the array
     */
    void Worker(int workerID) {
        // Pops tasks from the channel and stops when it receives a task of type Stop
        for (Task t{channels.at(workerID)->pop()}; t.type != Task::Type::Stop; t = channels.at(workerID)->pop()) {
            switch (t.type) {
                case Task::Type::ReduceStep:
                    // Sequential reduction
                    for (size_t i{t.first}; i < t.last; ++i) {
                        t.initialValue = oplus(t.initialValue, data.at(i));
                    }
                    partialResults.at(workerID) = t.initialValue;
                    b.wait();  // Waiting at the barrier
                    break;
                case Task::Type::InclusiveScan:
                    // Sequential inclusive scan
                    for (size_t i{t.first}; i < t.last; ++i) {
                        t.initialValue = oplus(t.initialValue, data.at(i));
                        data.at(i) = t.initialValue;
                    }
                    break;
                case Task::Type::ExclusiveScan:
                    // Sequential inclusive scan
                    if (t.last > t.first) {
                        for (size_t i{t.first}; i < t.last - 1; ++i) {
                            T temp = data.at(i);
                            data.at(i) = t.initialValue;
                            t.initialValue = oplus(t.initialValue, temp);
                        }
                        data.at(t.last - 1) = t.initialValue;
                    }
                    break;
                default:
                    break;
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
    explicit ParallelPrefix(bool _inclusive):
            inclusive(_inclusive) {
    }

    /**
     * A constructor to specify both the parallelism degree and the type of desired scan. Other parameters must be
     * provided at a later time.
     *
     * @param _inclusive        whether the scan should be inclusive or exclusive
     * @param _pardegree        the desired parallelism degree
     */
    ParallelPrefix(bool _inclusive, int _pardegree):
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
    ParallelPrefix(std::vector<T> _data, std::function<T(T,T)> _oplus, T _identity, bool _inclusive):
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
    ParallelPrefix(std::vector<T> _data,
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
     *  If computing it in parallel, it setups the thread pool as described above, then acts as a coordinator:
     *
     *  - partitioning the input array and distributing tasks for the phase 1
     *  - waiting for all threads to finish phase 1
     *  - performing a sequential exclusive scan on the intermediate results
     *  - distributing tasks for phase 2
     *  - waiting for all threads to finish.
     */
    void compute() {
        if (setUp) {
            if (pardegree == 0) sequentialScan();
            else {
                setupThreadPool();
                size_t sz = data.size() / pardegree;
                size_t rest = data.size() % pardegree;
                T start = initial;
                size_t prev = 0;
                // Phase 1
                for (size_t i{0}; i < pardegree; ++i) {
                    Task t;
                    t.initialValue = start;
                    t.type = Task::Type::ReduceStep;
                    t.first = prev;
                    // Fairer partitioning: instead of assigning (input size/parallelism degree) elements to the first
                    // (parallelism degree - 1) threads and the rest to the last one, it spreads the exceeding elements
                    // over the first ones.
                    t.last = (i == pardegree - 1) ? data.size() : prev + sz;
                    t.last = t.last + ((rest > 0) ? 1 : 0);
                    prev = t.last;
                    if (rest > 0) rest--;
                    channels.at(i)->push(t);
                }
                b.wait();  // Wait on the barrier
                T acc = initial;
                // Middle exclusive scan over the reduction intermediate results
                for (size_t i{0}; i < pardegree - 1; ++i) {
                    T temp = partialResults.at(i);
                    partialResults.at(i) = acc;
                    acc = oplus(acc, temp);
                }
                partialResults.at(pardegree - 1) = acc;
                prev = 0;
                rest = data.size() % pardegree;
                // Phase 2
                for (size_t i{0}; i < pardegree; ++i) {
                    Task t;
                    t.initialValue = partialResults.at(i);
                    t.type = (inclusive) ? Task::Type::InclusiveScan : Task::Type::ExclusiveScan;
                    t.first = prev;
                    t.last = (i == pardegree - 1) ? data.size() : prev + sz;
                    t.last = t.last + ((rest > 0) ? 1 : 0);
                    prev = t.last;
                    if (rest > 0) rest--;
                    channels.at(i)->push(t);
                }
                stop();
            }
        }
    }

    std::vector<T> popData() {
        setData = false;
        setUp = false;
        return std::move(data);
    }

};


#endif //SPM_FINAL_PROJECT_PARALLELPREFIX_H
