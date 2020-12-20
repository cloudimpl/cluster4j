package com.cloudimpl.db4j.core.btree3;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import java.util.Iterator;

public class SortedIterator<T extends Comparable<T>> implements Iterator<T> {
  private final PeekingIterator<T> peekingIterator1;
  private final PeekingIterator<T> peekingIterator2;

  public SortedIterator(Iterator<T> source1, Iterator<T> source2) {
    peekingIterator1 = Iterators.peekingIterator(source1);
    peekingIterator2 = Iterators.peekingIterator(source2);
  }

  @Override
  public boolean hasNext() {
    return peekingIterator1.hasNext() || peekingIterator2.hasNext();
  }

  @Override
  public T next() {
    if (!peekingIterator1.hasNext()) {
      return peekingIterator2.next();
    }
    if (!peekingIterator2.hasNext()) {
      return peekingIterator1.next();
    }

    T peek1 = peekingIterator1.peek();
    T peek2 = peekingIterator2.peek();
    if (peek1.compareTo(peek2) < 0) {
      return peekingIterator1.next();
    }
    return peekingIterator2.next();
  }
}