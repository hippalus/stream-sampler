package com.hippalus.streamsampler;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import redis.embedded.RedisServer;

@SpringBootTest
@EmbeddedKafka(brokerProperties = {
    "listeners=PLAINTEXT://localhost:9092",
    "port=9092"
})
@DirtiesContext
class CharacterStreamSamplerApplicationTests {

  private static RedisServer redisServer;

  @BeforeAll
  public static void setUp() {
    try {
      redisServer = RedisServer.builder().port(6379).build();
      redisServer.start();
    } catch (Exception e) {
      //do nothing
    }
  }

  @AfterAll
  public static void tearDown() {
    redisServer.stop();
  }

  @Test
  void contextLoads() {
  }

}
