package com.hippalus.streamsampler.sampling;

import java.util.stream.Stream;

public interface SamplePriorityQueue<E> {

  E remove();

  E peek();

  boolean add(E value);

  boolean isEmpty();

  int size();

  void clear();

  Stream<E> stream();
}
