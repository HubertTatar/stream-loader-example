package io.huta.sle.deduplication

import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet

import java.util

/** Not thread safe
  *
  * FIFO Hash set storing keys, removes last element while adding new element. Adds key to beginning, if key already
  * exists it will be moved to beginning of the set
  */
class FifoHashSet[A] private (maxEntries: Int, set: ObjectLinkedOpenHashSet[A]) extends util.AbstractSet[A] {

  override def add(e: A): Boolean = {
    if (size() >= maxEntries) {
      set.removeLast()
    }
    set.addAndMoveToFirst(e)
  }

  override def contains(o: Any): Boolean = set.contains(o)

  override def clear(): Unit = set.clear()

  override def size(): Int = set.size()

  override def iterator(): util.Iterator[A] = set.iterator()
}

object FifoHashSet {

  def apply[A](maxEntries: Int): FifoHashSet[A] = {
    val set = new ObjectLinkedOpenHashSet[A](maxEntries)
    new FifoHashSet(maxEntries, set)
  }
}
