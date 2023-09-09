//
// Created by lp on 2023/9/9.
//

#ifndef MM_THREADPOOL_H_
#define MM_THREADPOOL_H_

#include <atomic>       // std::atomic
#include <future>       // std::future
#include <memory>       // std::unique_ptr
#include <thread>       // std::thread
#include <type_traits>  // std::invoke_result_t
#include <utility>      // std::move
#include <vector>       // std::vector

#include "mm_threadsafe_deque.h"  // mm::LockBasedDeque
#include "mm_threadsafe_queue.h"  // mm::LockFreeQueue

namespace mm {

/**
 * A custom function wrapper which requires callable object are non-copyable but movable.
 *
 * On the contrary, std::function<> requires that the store function objects are copy-constructible.
 */
class FunctionWrapper {
 private:
  struct FuncInterface {
    virtual void Call() = 0;
    virtual ~FuncInterface() = default;
  };

  template <typename F>
  struct FuncImpl : public FuncInterface {
    explicit FuncImpl(F &&f) : f(::std::move(f)) {}
    void Call() override { f(); }
    F f;
  };

 public:
  FunctionWrapper() = default;
  FunctionWrapper(FunctionWrapper &&f) noexcept : m_func(::std::move(f.m_func)) {}

  /**
   * Create a function wrapper.
   *
   * TODO(Warning: TMP): Constructor accepting a forwarding reference can hide the move constructor.
   *
   * @tparam F The type of the callable object.
   * @param f The callable object.
   */
  template <typename F>
  explicit FunctionWrapper(F &&f) : m_func(new FuncImpl<F>(::std::forward<F>(f))) {}  // NOLINT.

  FunctionWrapper &operator=(FunctionWrapper &&f) noexcept {
    m_func = ::std::move(f.m_func);
    return *this;
  }

  FunctionWrapper(FunctionWrapper const &) = delete;
  FunctionWrapper &operator=(FunctionWrapper const &) = delete;

  ~FunctionWrapper() = default;

 public:
  void operator()() { m_func->Call(); }

 private:
  ::std::unique_ptr<FuncInterface> m_func = nullptr;
};

/**
 * A simple unoptimized thread pool.
 *
 * Each worker thread in the thread pool has its own local task queue. If each worker thread has a
 * task to submit, it will submit it to its own local task queue. When each worker thread executes a
 * task, it will first fetch the task from the local task queue. If there is no task in the local
 * task queue, it will get the task from the global task queue. If there is no task in the global
 * task queue, a task will be stolen from the local task queue of other threads for execution.
 */
class ThreadPool {
 public:
  using LocalTaskQueueType = LockBasedDeque<FunctionWrapper>;
  using GlobalTaskQueueType = LockFreeQueue<FunctionWrapper>;

 public:
  ThreadPool() : m_bDone(false) {
    unsigned int const threadCount = ::std::thread::hardware_concurrency();
    try {
      // Create local task queues for every thread.
      for (unsigned int i = 0; i < threadCount; ++i) {
        m_pAllLocalTaskQueues.emplace_back(::std::make_unique<LocalTaskQueueType>());
      }
      // Create threads.
      for (unsigned int i = 0; i < threadCount; ++i) {
        m_threads.emplace_back(&ThreadPool::Worker, this, i);
      }
    } catch (...) {
      m_bDone = true;
      throw;
    }
  }

  ~ThreadPool() {
    m_bDone = true;
    Join();
  }

 public:
  /**
   * Submit a task.
   *
   * @tparam F The type of the task.
   * @param func task.
   * @return ::std::future.
   */
  template <typename F>
  ::std::future<::std::invoke_result_t<F>> Submit(F func) {
    using ResultType = ::std::invoke_result_t<F>;

    ::std::packaged_task<ResultType()> task(::std::forward<F>(func));
    ::std::future<ResultType> res(task.get_future());

    FunctionWrapper funcWrapper(::std::move(task));
    // Always try to push tasks on local queue first.
    if (m_pLocalTaskQueue) {
      m_pLocalTaskQueue->PushBack(::std::move(funcWrapper));
    } else {
      m_GlobalTaskQueue.Push(::std::move(funcWrapper));
    }

    return res;
  }

  /**
   * Run a pending task in the work queue.
   *
   * Another scene of calling this function separately: If the current thread needs to wait for
   * other tasks in the work queue to be completed, then for efficiency, this thread can also try to
   * help to take the pending tasks from the work queue to execute, instead of waiting.
   */
  void RunOnePendingTask() {
    FunctionWrapper task;

    if (GetTaskFromLocalQueue(task) || GetTaskFromGlobalQueue(task) ||
        GetTaskFromOtherThreadLocalQueue(task)) {
      task();
    } else {
      ::std::this_thread::yield();
    }
  }

 private:
  /**
   * Worker.
   */
  void Worker(unsigned int idx) {
    // Get the local task queue to which this thread belongs.
    m_idxOfLocalTaskQueues = idx;
    m_pLocalTaskQueue = m_pAllLocalTaskQueues[idx].get();

    while (!m_bDone) {
      RunOnePendingTask();
    }
  }

  /**
   * Try to get a task from the local queue.
   *
   * @param task [out]
   * @return False if the local queue is empty, true if successful.
   */
  bool GetTaskFromLocalQueue(FunctionWrapper &task) {  // NOLINT.
    return m_pLocalTaskQueue && m_pLocalTaskQueue->TryPopFront(task);
  }

  /**
   * Try to get a task from the global queue.
   *
   * @param task [out]
   * @return False if failed, true if successful.
   */
  bool GetTaskFromGlobalQueue(FunctionWrapper &task) {
    auto pTask = m_GlobalTaskQueue.Pop();
    if (!pTask) {
      return false;
    }
    task = ::std::move(*pTask);
    pTask.release();  // NOLINT.
    return true;
  }

  /**
   * Try to get a task from the local queue of another thread.
   *
   * @param task [out]
   * @return False if failed, true if successful.
   */
  bool GetTaskFromOtherThreadLocalQueue(FunctionWrapper &task) {
    unsigned int idxOfOthers = 0;
    size_t numOfQueues = m_pAllLocalTaskQueues.size();
    for (unsigned int i = 0; i < numOfQueues; ++i) {
      idxOfOthers = (m_idxOfLocalTaskQueues + i + 1) % numOfQueues;
      if (m_pAllLocalTaskQueues[idxOfOthers] &&
          m_pAllLocalTaskQueues[idxOfOthers]->TryPopBack(task)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Join all thread before existing.
   */
  void Join() {
    for (auto &thread : m_threads) {
      thread.join();
    }
  }

 private:
  // The Flag to indicate whether quiting.
  ::std::atomic_bool m_bDone;
  // The global task queue for common use.
  GlobalTaskQueueType m_GlobalTaskQueue;
  // All local task queues that threads use.
  ::std::vector<::std::unique_ptr<LocalTaskQueueType>> m_pAllLocalTaskQueues;
  // Work threads.
  ::std::vector<::std::thread> m_threads;

  // The index of the local task queue to which the thread belongs in the all local task queues.
  static thread_local unsigned int m_idxOfLocalTaskQueues;
  // The pointer to the local task queue to which the thread belongs.
  static thread_local LocalTaskQueueType *m_pLocalTaskQueue;
};

thread_local unsigned int ThreadPool::m_idxOfLocalTaskQueues = 0;
thread_local ThreadPool::LocalTaskQueueType *ThreadPool::m_pLocalTaskQueue = nullptr;

}  // namespace mm

#endif  // MM_THREADPOOL_H_
