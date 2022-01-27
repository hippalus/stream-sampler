package com.hippalus.streamsampler.sampling.streams;

import com.hippalus.streamsampler.AbstractIntegrationTest;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

@Slf4j
class StreamSamplerIntegrationTest extends AbstractIntegrationTest {

  @Autowired
  private StreamSampler<Character> characterStreamSampler;
  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  @BeforeAll
  public static void beforeAll() {
    AbstractIntegrationTest.startCluster();
  }

  @AfterAll
  public static void afterAll() {
    AbstractIntegrationTest.stopCluster();
  }

  @BeforeEach
  public void prepareKafkaCluster() throws Exception {
    CLUSTER.deleteTopicsAndWait(60000, "characters-integration-test");
    CLUSTER.createTopic("characters-integration-test");
  }

  @AfterEach
  public void shutdown() {
    characterStreamSampler.stop();
  }

  @Test
  void shouldConsumeSourceKafkaTopicAndRandomlySampleFromKStream() throws InterruptedException {
    //given
    IntStream.range(0, 100)
        .mapToObj(value -> RandomStringUtils.randomAlphabetic(100))
        .forEach(s -> kafkaTemplate.send("characters-integration-test", s));

    //when
    new Thread(() -> characterStreamSampler.start(10)).start();

    //and then
    TestUtils.waitForCondition(() -> getCurrentSample().length() == 10, 50000, "Timed out getting currentSample");
    final String currentSample = getCurrentSample();
    log.info("Sample {}", currentSample);

    //then
    Assertions.assertEquals(10, currentSample.length());


  }

  private String getCurrentSample() {
    return characterStreamSampler.currentSample()
        .stream()
        .map(Object::toString)
        .collect(Collectors.joining());
  }

}
