package com.hippalus.streamsampler.sampling;

import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

public interface RandomSampler<T> {

  void feed(T input);

  default void feed(Iterator<T> input) {
    while (input.hasNext()) {
      feed(input.next());
    }
  }

  default void feed(Iterable<T> input) {
    input.forEach(this::feed);
  }

  default void feed(Stream<T> input) {
    input.forEach(this::feed);
  }

  int sampleSize();

  Collection<T> sample();

  void clear();
}
