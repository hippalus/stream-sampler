package com.hippalus.streamsampler.sampling;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

public class SamplerLockRedisAdapter implements SamplerLock {

  private final RLock lock;

  public SamplerLockRedisAdapter(RedissonClient redissonClient) {
    this.lock = redissonClient.getLock("sampler-lock");
  }

  @Override
  public boolean tryLock() {
    return lock.tryLock();
  }

  @Override
  public void unlock() {
    lock.unlock();
  }
}
