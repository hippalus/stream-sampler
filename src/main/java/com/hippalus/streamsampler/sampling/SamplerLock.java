package com.hippalus.streamsampler.sampling;

public interface SamplerLock {

  boolean tryLock();

  void unlock();
}
