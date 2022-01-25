package com.hippalus.streamsampler.sampling;

import java.util.stream.Stream;
import org.redisson.api.RPriorityQueue;
import org.redisson.api.RedissonClient;

public class SamplePriorityQueueRedisAdapter<E> implements SamplePriorityQueue<E> {

  private final RPriorityQueue<E> queue;

  public SamplePriorityQueueRedisAdapter(RedissonClient redissonClient) {
    this(redissonClient.getPriorityQueue("sample-queue"));
  }

  public SamplePriorityQueueRedisAdapter(RPriorityQueue<E> queue) {
    this.queue = queue;
  }

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
  public Stream<E> stream() {
    return queue.stream();
  }
}
