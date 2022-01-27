package com.hippalus.streamsampler.sampling.streams;

import com.hippalus.streamsampler.AbstractIntegrationTest;
import java.util.Collection;
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

    final int expectedSampleSize = 15;
    new Thread(() -> characterStreamSampler.start(expectedSampleSize)).start();

    //and
    TestUtils.waitForCondition(() -> characterStreamSampler.currentSample().size() == expectedSampleSize, 500000,
        "Timed out getting currentSample");

    //when
    final Collection<Character> currentSample = characterStreamSampler.currentSample();
    final int actualSampleSize = currentSample.size();
    log.info("Sample {} size {}", currentSample, actualSampleSize);

    //then
    Assertions.assertEquals(expectedSampleSize, actualSampleSize);
  }

}
