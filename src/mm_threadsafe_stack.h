//
// Created by lp on 2023/8/29.
//

#ifndef MM_THREADSAFE_STACK_H_
#define MM_THREADSAFE_STACK_H_

#include <atomic>   // std::atomic
#include <memory>   // std::unique_ptr
#include <tuple>    // std::ignore
#include <utility>  // std::move

namespace mm {

/**
 * A lockfree stack using split reference counts.
 *
 * In order to correctly handle the memory allocation and release of stack nodes. The stack node
 * contains two counts: an internal count and an external count. The value of the internal count
 * plus the value of the external count is equal to the number of references to the node. When the
 * node is read, the external count is incremented by one, and when the reading finished, the
 * internal count is decremented by one. When the node needs to be deleted, it will check whether
 * the value of internal count plus external count minus one is 0. If it is 0, it means that the
 * node is not currently referenced, so it can be safely deleted.
 *
 * Note: If you want it to be lockfree, the platform must support the double-word-compare-and-swap
 * operation.
 * @tparam T
 */
template <typename T>
class LockFreeStack {
 private:
  struct Node;

  /**
   * NodeWrapper is TriviallyCopyable and small enough for std::atomic<NodeWrapper> to be
   * lockfree.(the platform that supports the double-word-compare-and-swap operation). However, if
   * the platform does not support this operation, it will not be the lockfree stack because the
   * std::atomic will use a mutex to guarantee atomicity when the type is too large.
   *
   * It keeps an external count that is equal to the number of those that are reading the node and a
   * pointer to the real node that stores the data.
   */
  struct NodeWrapper {
    int externalCount{1};
    Node *pNode{nullptr};
  };

  /**
   * The real node that stores the data.
   *
   * It keeps an internal count that is equal to the number of those that already finished reading
   * the node.
   */
  struct Node {
    explicit Node(T const &data) : pData(::std::make_unique<T>(data)) {}

    ::std::unique_ptr<T> pData{nullptr};
    ::std::atomic<int> internalCount{0};
    NodeWrapper nextNode{};
  };

 public:
  LockFreeStack() = default;
  ~LockFreeStack() {
    while (Pop())
      ;
  }

  LockFreeStack(LockFreeStack const &) = delete;
  LockFreeStack(LockFreeStack &&) = delete;
  LockFreeStack &operator=(LockFreeStack const &) = delete;
  LockFreeStack &operator=(LockFreeStack &&) = delete;

 public:
  /**
   * Push a node with the data on the stack.
   *
   * @param data The data that be pushed on the stack.
   */
  void Push(T const &data) {
    // Create a new node before push it on the stack, so it is exception safe when pop it because we
    // only need to pass the pointer out and there is no copying.(Exception will only occur here
    // when create it before pushing.)
    NodeWrapper newNodeWrapper;

    // Because the newNodeWrapper will be the new header, thus there is one external reference to
    // it(that is m_head).
    newNodeWrapper.externalCount = 1;
    newNodeWrapper.pNode = new Node(data);
    newNodeWrapper.pNode->nextNode = m_head.load(::std::memory_order::relaxed);
    while (!m_head.compare_exchange_weak(newNodeWrapper.pNode->nextNode, newNodeWrapper,
                                         ::std::memory_order::release,
                                         ::std::memory_order::relaxed))
      ;
  }

  /**
   * Pop a node with the data from the stack.
   *
   * @return The unique_ptr of the data.
   */
  ::std::unique_ptr<T> Pop() {
    NodeWrapper headNodeWrapper;
    while (true) {
      // Increase the external reference by one to indicate that we are referencing to it, and to
      // ensure that we can dereference it safely.
      headNodeWrapper = IncreaseHeadExternalCount();
      Node *pHeadNode = headNodeWrapper.pNode;
      // Test whether the stack is empty.
      if (!pHeadNode) {
        // We don't need to worry about that the number of external count of the dummy node(There
        // exists a dummy node when the stack is empty) overflows, because we don't need it to store
        // any data nor use it.
        return nullptr;
      }

      // Test whether the head is modified by other threads, if not, modify it by this thread.
      //
      // Here we must dereference pHeadNode, so we need to ensure that pHeadNode exists
      // and cannot be deleted by another thread, which is why we use reference counting to
      // implement memory management here.
      if (m_head.compare_exchange_strong(headNodeWrapper, pHeadNode->nextNode,
                                         ::std::memory_order::relaxed)) {
        ::std::unique_ptr<T> res = ::std::move(pHeadNode->pData);

        // Minus 2 because both m_head and headNodeWrapper no longer need to refer to it, so both
        // need to subtract the number of references
        int increaseCount = headNodeWrapper.externalCount - 2;
        // Test whether other threads are still referencing the node, if not, it can be safely
        // deleted.
        if (pHeadNode->internalCount.fetch_add(increaseCount, ::std::memory_order::release) ==
            -increaseCount) {
          delete pHeadNode;
        }
        return res;
      }

      // Minus the reference to it from headNodeWrapper.
      //
      // This action will also ensure that the last thread that references to the node will delete
      // it. There is no need to worry that other threads will reference the node again during the
      // deletion process, because if pHeadNode->internalCount.fetch_add(-1) == 1 is true, it means
      // that the head node m_head no longer refers to it(only current thread references to it), so
      // other threads cannot get the reference of the node again through m_head, so the node can be
      // safely deleted.
      if (pHeadNode->internalCount.fetch_add(-1, ::std::memory_order::relaxed) == 1) {
        // Ignore the return value. Just for matching the
        // pHeadNode->internalCount.fetch_add(increaseCount, ::std::memory_order::release) to make
        // std::move happen before delete.
        std::ignore = pHeadNode->internalCount.load(::std::memory_order::acquire);
        delete pHeadNode;
      }
    }
  }

 private:
  /**
   * Increase the external count of the header by one.
   *
   * @return The new header that the external count is increased by one.
   */
  NodeWrapper IncreaseHeadExternalCount() {
    NodeWrapper oldHeadNodeWrapper = m_head.load(::std::memory_order::relaxed);
    NodeWrapper newHeadNodeWrapper;
    do {
      newHeadNodeWrapper = oldHeadNodeWrapper;
      ++newHeadNodeWrapper.externalCount;
    } while (!m_head.compare_exchange_strong(oldHeadNodeWrapper, newHeadNodeWrapper,
                                             ::std::memory_order::acquire,
                                             ::std::memory_order::relaxed));

    return newHeadNodeWrapper;
  }

 private:
  ::std::atomic<NodeWrapper> m_head{};
};

}  // namespace mm

#endif  // MM_THREADSAFE_STACK_H_
