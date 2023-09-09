//
// Created by lp on 2023/8/31.
//

#ifndef MM_THREADSAFE_QUEUE_H_
#define MM_THREADSAFE_QUEUE_H_

#include <atomic>  // std::atomic
#include <memory>  // std::unique_ptr

namespace mm {

template <typename T>
class LockFreeQueue {
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
    int externalCount{0};
    Node *pNode{nullptr};
  };

  /**
   * We need to update the internal count and external counter together as a single entity because
   * of the data race. Make it as an atomic.
   */
  struct NodeCounter {
    // Keep the total size to 32 bit(make the structure within a machine word makes it more likely
    // lockfree), and we also need to leave plenty of scope for large internal count values. So, we
    // specify the internal count as a 30 bit value.
    unsigned internalCount : 30;
    // There are only two external counters here: head and tail, so only 2 bits are needed.
    unsigned externalCounters : 2;
  };

  /**
   * The real node that stores the data.
   *
   * Reference: <mm_threadsafe_stack.h>
   * It keeps a count which includes two parts: one part is an internal count that is equal to the
   * number of those(threads) that already finished reading the node(same as LockFreeStack). Another
   * part presents the number of external counters: in here they are head and tail(Any node can only
   * be read through head or tail). Unlike LockFreeStack, LockFreeQueue has one more variable tail,
   * so we need to add an external count in tail the same as the head in LockFreeStack. If the value
   * of this external counters equals to 0, it presents the node being popped out and the head and
   * tail are not reference to it anymore. If the value equals to 1, present the node has been
   * pushed in and not been popped out. If the value equals to 2, present the node is not pushed in.
   */
  struct Node {
    Node() {
      NodeCounter initialCount{0, 2};
      count.store(initialCount);
    }

    ::std::atomic<NodeCounter> count;
    ::std::atomic<T *> pData{nullptr};
    ::std::atomic<NodeWrapper> next;
  };

 public:
  LockFreeQueue() {
    NodeWrapper nodeWrapper = {1, nullptr};
    nodeWrapper.pNode = new Node;
    m_head.store(nodeWrapper);
    m_tail.store(nodeWrapper);
  };
  ~LockFreeQueue() {
    while (Pop())
      ;
    Node *pNode = m_tail.load().pNode;
    delete pNode;
  }

  LockFreeQueue(LockFreeQueue const &) = delete;
  LockFreeQueue(LockFreeQueue &&) = delete;
  LockFreeQueue &operator=(LockFreeQueue const &) = delete;
  LockFreeQueue &operator=(LockFreeQueue &&) = delete;

 public:
  /**
   * Push a node on the queue.
   *
   * @param data
   */
  void Push(T data) {
    // Prepare the new data in advance.
    auto pNewData = ::std::make_unique<T>(::std::move(data));
    NodeWrapper nextNodeWrapper;
    nextNodeWrapper.pNode = new Node;
    nextNodeWrapper.externalCount = 1;

    NodeWrapper oldTail;
    while (true) {
      // Increase a reference to the tail, so we can dereference it safely.
      oldTail = IncreaseExternalCount(m_tail);
      T *pOldData = nullptr;
      // Use the CAS operation to get the ownership of the tail if the oldTail is still the real
      // tail(It is not modified by other threads).
      if (oldTail.pNode->pData.compare_exchange_strong(pOldData, pNewData.get())) {
        NodeWrapper emptyNodeWrapper = {0};
        // If another thread helped to create the next NodeWrapper, it does not need to create it
        // again.
        if (!oldTail.pNode->next.compare_exchange_strong(emptyNodeWrapper, nextNodeWrapper)) {
          // Get the next node that another thread has set.
          delete nextNodeWrapper.pNode;
          nextNodeWrapper = emptyNodeWrapper;
        }
        // Set the new tail.
        SetNewTail(oldTail, nextNodeWrapper);
        pNewData.release();
        break;
      } else {
        // Failed to set the data. But it can help the successful thread to complete the update.
        NodeWrapper emptyNodeWrapper = {0};
        // Try to update.
        if (oldTail.pNode->next.compare_exchange_strong(emptyNodeWrapper, nextNodeWrapper)) {
          emptyNodeWrapper = nextNodeWrapper;
          // Create a new node for next loop.
          nextNodeWrapper.pNode = new Node;
        }
        SetNewTail(oldTail, emptyNodeWrapper);
      }
    }
  }

  ::std::unique_ptr<T> Pop() {
    NodeWrapper oldHead;
    while (true) {
      oldHead = IncreaseExternalCount(m_head);
      Node *const pOldHeadNode = oldHead.pNode;

      // If the queue is empty.
      if (pOldHeadNode == m_tail.load().pNode) {
        DecreaseInternalCount(pOldHeadNode);
        return nullptr;
      }

      // If the oldHead is the real head(It is not modified by other threads), get the ownership of
      // the head, and it will also move the head to the next node.
      NodeWrapper next = pOldHeadNode->next.load();
      if (m_head.compare_exchange_strong(oldHead, next)) {
        // Get the data.
        T *const data = pOldHeadNode->pData.exchange(nullptr);
        FreeExternalCounter(oldHead);
        return ::std::unique_ptr<T>(data);
      }

      // If the oldHead is not the real head, just release the reference to it.
      DecreaseInternalCount(pOldHeadNode);
    }
  }

 private:
  /**
   * Increase the external count of the node by one.
   *
   * @param node The node that you want to increase external count by one.
   * @return The node that the external count is increased by one.
   */
  NodeWrapper IncreaseExternalCount(::std::atomic<NodeWrapper> &nodeWrapper) {
    NodeWrapper newNodeWrapper;
    NodeWrapper oldNodeWrapper = nodeWrapper.load(::std::memory_order::relaxed);
    do {
      newNodeWrapper = oldNodeWrapper;
      ++newNodeWrapper.externalCount;
    } while (!nodeWrapper.compare_exchange_strong(oldNodeWrapper, newNodeWrapper,
                                                  ::std::memory_order::acquire,
                                                  ::std::memory_order::relaxed));

    return newNodeWrapper;
  }

  /**
   * Decrease the internal count of the node by one.
   *
   * @param nodeWrapper
   */
  void DecreaseInternalCount(Node *pNode) {
    NodeCounter newNodeCounter;
    NodeCounter oldNodeCounter = pNode->count.load(::std::memory_order::relaxed);

    do {
      newNodeCounter = oldNodeCounter;
      --newNodeCounter.internalCount;
    } while (!pNode->count.compare_exchange_strong(oldNodeCounter, newNodeCounter,
                                                   ::std::memory_order::acquire,
                                                   ::std::memory_order::relaxed));

    if (!newNodeCounter.internalCount && !newNodeCounter.externalCounters) {
      delete pNode;
    }
  }

  /**
   * Free an external counter to a node. It will update the values of external counter and internal
   * count at the same time.
   *
   * @param nodeWrapper The node wrapper that you want to free the external counter.
   */
  void FreeExternalCounter(NodeWrapper &nodeWrapper) {
    Node *const pNode = nodeWrapper.pNode;
    int increaseCount = nodeWrapper.externalCount - 2;
    NodeCounter newNodeCounter;
    NodeCounter oldNodeCounter = pNode->count.load(::std::memory_order::relaxed);

    do {
      newNodeCounter = oldNodeCounter;
      --newNodeCounter.externalCounters;
      newNodeCounter.internalCount += increaseCount;
    } while (!pNode->count.compare_exchange_strong(oldNodeCounter, newNodeCounter,
                                                   ::std::memory_order::acquire,
                                                   ::std::memory_order::relaxed));

    // If the values of external counters and internal count equal 0, then there are no other
    // references to the node, and it can be deleted safely.
    if (!newNodeCounter.externalCounters && !newNodeCounter.internalCount) {
      delete pNode;
    }
  }

  /**
   * Set the new tail.
   *
   * @param oldTail
   * @param newTail
   */
  void SetNewTail(NodeWrapper &oldTail, NodeWrapper const &newTail) {
    Node *const pOldTailNode = oldTail.pNode;
    while (!m_tail.compare_exchange_weak(oldTail, newTail) && oldTail.pNode == pOldTailNode)
      ;
    if (oldTail.pNode == pOldTailNode) {
      // Successfully set the tail, and we need to free the external counter of old tail.
      FreeExternalCounter(oldTail);
    } else {
      // Fail to set the tail, another thread will free the external counter, and we only need to
      // release the single reference held by this thread.
      DecreaseInternalCount(pOldTailNode);
    }
  }

 private:
  ::std::atomic<NodeWrapper> m_head{};
  ::std::atomic<NodeWrapper> m_tail{};
};

}  // namespace mm

#endif  // MM_THREADSAFE_QUEUE_H_
