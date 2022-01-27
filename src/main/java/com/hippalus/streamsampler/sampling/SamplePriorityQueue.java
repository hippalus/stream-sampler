package com.hippalus.streamsampler.sampling;

import java.util.List;

public interface SamplePriorityQueue<E> {

  E remove();

  E peek();

  boolean add(E value);

  boolean isEmpty();

  int size();

  void clear();

  List<E> readAll();
}
