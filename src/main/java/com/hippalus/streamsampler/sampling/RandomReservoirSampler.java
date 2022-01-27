package com.hippalus.streamsampler.sampling;

import java.util.Collection;
import java.util.random.RandomGenerator;
import java.util.random.RandomGeneratorFactory;

public class RandomReservoirSampler<T> implements RandomSampler<T> {

  private final int sampleSize;
  private final RandomGenerator random;
  private final SamplePriorityQueue<Sample<T>> sampleQueue;
  private final SamplerLock samplerLock;

  public RandomReservoirSampler(int sampleSize, SamplePriorityQueue<Sample<T>> queue, SamplerLock samplerLock) {
    this(sampleSize, RandomGeneratorFactory.of("Xoshiro256PlusPlus").create(), queue, samplerLock);
  }

  public RandomReservoirSampler(int sampleSize, RandomGenerator random, SamplePriorityQueue<Sample<T>> queue,
      SamplerLock samplerLock) {
    this.sampleSize = sampleSize;
    this.random = random;
    this.sampleQueue = queue;
    this.samplerLock = samplerLock;
  }

  @Override
  public void feed(T input) {
    if (samplerLock.tryLock()) {
      try {
        Sample<T> smallest = getSmallest(input);
        for (int i = 0; i < sampleSize; i++) {
          final double randWeight = random.nextDouble();
          assert smallest != null;
          if (randWeight > smallest.getWeight()) {
            sampleQueue.remove();
            sampleQueue.add(Sample.of(input, randWeight));
            smallest = sampleQueue.peek();
          }
        }
      } finally {
        samplerLock.unlock();
      }
    }
  }

  @Override
  public int sampleSize() {
    return sampleSize;
  }

  @Override
  public Collection<T> sample() {
    return sampleQueue.stream().map(Sample::getElement).toList();
  }

  @Override
  public void clear() {
    sampleQueue.clear();
  }

  private Sample<T> getSmallest(T value) {
    return sampleQueue.isEmpty() ? initQueueAndGetSmallestSample(value) : sampleQueue.peek();
  }

  private Sample<T> initQueueAndGetSmallestSample(T value) {
    if (samplerLock.tryLock()) {
      Sample<T> smallest = null;
      try {
        for (int i = 0; i < sampleSize; i++) {
          if (sampleQueue.size() == sampleSize) {
            break;
          }
          sampleQueue.add(Sample.of(value, random.nextDouble()));
          smallest = sampleQueue.peek();
        }
      } finally {
        samplerLock.unlock();
      }
      return smallest;
    }
    return null;
  }

}
