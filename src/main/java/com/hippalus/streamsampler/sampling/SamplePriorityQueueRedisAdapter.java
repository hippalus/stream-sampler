package com.hippalus.streamsampler.sampling;

import java.util.List;
import org.redisson.api.RPriorityBlockingQueue;
import org.redisson.api.RedissonClient;

public class SamplePriorityQueueRedisAdapter<E> implements SamplePriorityQueue<E> {

  private final RPriorityBlockingQueue<E> queue;

  public SamplePriorityQueueRedisAdapter(RedissonClient redissonClient) {
    this(redissonClient.getPriorityBlockingQueue("sample-queue"));
  }

  public SamplePriorityQueueRedisAdapter(RPriorityBlockingQueue<E> queue) {
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
  public List<E> readAll() {
    return queue.readAll();
  }
}
