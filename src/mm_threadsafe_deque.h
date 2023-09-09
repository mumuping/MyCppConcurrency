//
// Created by lp on 2023/9/9.
//

#ifndef MM_THREADSAFE_DEQUE_H_
#define MM_THREADSAFE_DEQUE_H_

#include <deque>  // std::deque
#include <mutex>  // std::mutex

/**
 * A simple threadsafe deque based on lock.
 *
 * It is inefficient and not recommended to use.
 *
 * @tparam T
 */
template <typename T>
class LockBasedDeque {
 public:
  LockBasedDeque() = default;
  ~LockBasedDeque() = default;

  LockBasedDeque(LockBasedDeque &&other) noexcept {
    ::std::lock_guard<::std::mutex> lk(other.m_latch);
    m_data = ::std::move(other.m_data);
  }
  LockBasedDeque(LockBasedDeque const &other) {
    ::std::lock_guard<::std::mutex> lk(other.m_latch);
    m_data = other.m_data;
  }

  LockBasedDeque &operator=(LockBasedDeque const &) = delete;
  LockBasedDeque &operator=(LockBasedDeque &&) = delete;

 public:
  /**
   * Push the data on the front of the deque.
   *
   * @param data
   */
  void PushFront(T data) {
    ::std::lock_guard<::std::mutex> lk(m_latch);
    m_data.emplace_front(::std::move(data));
  }

  /**
   * Push the data on the back of the deque.
   *
   * @param data
   */
  void PushBack(T data) {
    ::std::lock_guard<::std::mutex> lk(m_latch);
    m_data.emplace_back(::std::move(data));
  }

  /**
   * Try to pop out the data on front of the deque.
   *
   * @param outData [out]
   * @return False if empty, true if successful.
   */
  bool TryPopFront(T &outData) {
    ::std::lock_guard<::std::mutex> lk(m_latch);
    if (m_data.empty()) {
      return false;
    }
    outData = ::std::move(m_data.front());
    m_data.pop_front();
    return true;
  }

  /**
   * Try to pop out the data on front of the deque.
   *
   * @param outData [out]
   * @return False if empty, true if successful.
   */
  bool TryPopBack(T &outData) {
    ::std::lock_guard<::std::mutex> lk(m_latch);
    if (m_data.empty()) {
      return false;
    }
    outData = ::std::move(m_data.back());
    m_data.pop_back();
    return true;
  }

  /**
   * Emtpy or not.
   * @return
   */
  bool Empty() const {
    ::std::lock_guard<::std::mutex> lk(m_latch);
    return m_data.empty();
  }

 private:
  ::std::deque<T> m_data;
  mutable ::std::mutex m_latch;
};

#endif  // MM_THREADSAFE_DEQUE_H_
