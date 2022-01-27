package com.hippalus.streamsampler.sampling.streams;

import java.util.Collection;

public interface StreamSampler<T> {

  void start(int sampleSize);

  void stop();

  Collection<T> currentSample();

  void cleanLocalState();

  default void addShutdownHookAndBlock(final StreamSampler<?> streamSampler) {
    Thread.currentThread().setUncaughtExceptionHandler((t, e) -> streamSampler.stop());
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        streamSampler.stop();
      } catch (final Exception ignored) {
        //NOOP
      }
    }));
    joinCurrentThread();
  }

  private void joinCurrentThread() {
    try {
      Thread.currentThread().join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
