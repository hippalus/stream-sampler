package com.hippalus.streamsampler.sampling;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
class RandomSamplerTest {

  private static final int SOURCE_SIZE = 10000;
  private static final List<Character> SOURCE = new ArrayList<>(SOURCE_SIZE);

  @BeforeAll
  static void setUp() {
    RandomStringUtils.randomAlphabetic(10000)
        .chars()
        .mapToObj(value -> (char) value)
        .forEach(SOURCE::add);
  }

  @AfterAll
  static void tearDown() {
  }

  @Test
  void testReservoirSamplerSampledSize() {
    verifySamplerFixedSampleSize(1);
    verifySamplerFixedSampleSize(10);
    verifySamplerFixedSampleSize(100);
    verifySamplerFixedSampleSize(1234);
    verifySamplerFixedSampleSize(9999);
    verifySamplerFixedSampleSize(20000);
  }

  @Test
  void testSampledOutputSizeShouldNotBeyondTheSourceSize() {
    final RandomSampler<Character> sampler = newRandomReservoirSampler(10000);
    sampler.feed(SOURCE.iterator());
    final Collection<Character> sampled = sampler.sample();
    assertEquals(SOURCE_SIZE, sampled.size());
  }


  private void verifySamplerFixedSampleSize(int sampleSize) {
    final RandomSampler<Character> sampler = newRandomReservoirSampler(sampleSize);
    sampler.feed(SOURCE.stream());
    final Collection<Character> sampled = sampler.sample();
    assertNotNull(sampled);
    assertEquals(sampleSize, sampled.size());
  }


  private RandomSampler<Character> newRandomReservoirSampler(int sampleSize) {
    return new RandomReservoirSampler<>(sampleSize, new SamplePriorityQueueInMemoryAdapter<>(), new SamplerLockNoopAdapter());
  }


  static class SamplePriorityQueueInMemoryAdapter<E> implements SamplePriorityQueue<E> {

    private final PriorityQueue<E> queue = new PriorityQueue<>();

    @Override
    public E remove() {
      return queue.remove();
    }

    @Override
    public E peek() {
      return queue.peek();
    }

    @Override
    public boolean add(E value) {
      return queue.add(value);
    }

    @Override
    public boolean isEmpty() {
      return queue.isEmpty();
    }

    @Override
    public int size() {
      return queue.size();
    }

    @Override
    public void clear() {
      queue.clear();
    }

    @Override
    public List<E> readAll() {
      return queue.stream().toList();
    }
  }

  static class SamplerLockNoopAdapter implements SamplerLock {

    @Override
    public boolean tryLock() {
      return true;
    }

    @Override
    public void unlock() {
      //do nothing
    }
  }
}
