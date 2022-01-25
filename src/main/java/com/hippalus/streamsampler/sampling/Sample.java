package com.hippalus.streamsampler.sampling;

import java.io.Serial;
import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Sample<T> implements Comparable<Sample<T>>, Serializable {

  @Serial
  private static final long serialVersionUID = 1L;

  private T element;
  private double weight;

  public static <T> Sample<T> of(T element, double weight) {
    return new Sample<>(element, weight);
  }

  @Override
  public int compareTo(Sample<T> o) {
    return this.weight >= o.getWeight() ? 1 : -1;
  }
}
