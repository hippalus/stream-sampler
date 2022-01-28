package com.hippalus.streamsampler.sampling;

import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class Utils {

  private Utils() {
    throw new AssertionError();
  }

  public static Character castToChar(int i) {
    try {
      return (char) i;
    } catch (Exception exception) {
      log.error("Exception has been occurred while casting {} to char. {}", i, exception.getMessage());
    }
    return null;
  }

  public static void sleep(long timeout) {
    try {
      TimeUnit.MILLISECONDS.sleep(timeout);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

}
