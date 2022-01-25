package com.hippalus.streamsampler.cli;

import com.hippalus.streamsampler.sampling.streams.StreamSampler;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@Profile("!test")
@RequiredArgsConstructor
public class CommandLineCharacterStreamSamplerRunner implements ApplicationRunner {

  @Value("${application.stream-sampler.inbound-topic}")
  private String characterTopicName;

  private final StreamSampler<Character> characterStreamSampler;
  private final KafkaTemplate<String, String> kafkaTemplate;

  @SuppressWarnings("InfiniteLoopStatement")
  @Override
  public void run(ApplicationArguments args) throws Exception {

    new Thread(readStdInAndFeedCharTopic()).start();
/*    if (!args.containsOption("--sample.size")) {
      log.error("Usage:%n  java -jar application.jar <sampleSize>");
      System.exit(1);
    }

    final int sampleSize = Integer.parseInt(args.getOptionValues("--sample.size").get(0));
    if (args.containsOption("file")) {
      //TODO: read file
    } else {

    }*/
    // final Integer integer = Arrays.stream(args.getSourceArgs()).findFirst().map(Integer::parseInt).get();
    new Thread(() -> {
      while (true) {
        final String sample = characterStreamSampler.currentSample()
            .stream()
            .map(Object::toString)
            .collect(Collectors.joining());
        log.info("Current Sample {}", sample);
        sleep();
      }
    }).start();

    characterStreamSampler.start(5);
  }

  private void sleep() {
    try {
      TimeUnit.MILLISECONDS.sleep(10000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private Runnable readStdInAndFeedCharTopic() {
    return () -> {
      try (final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in))) {
        bufferedReader.lines().forEach(line -> kafkaTemplate.send(new ProducerRecord<>(characterTopicName, null, line)));
      } catch (IOException e) {
        log.error("Exception has been occurred while reading stdin", e);
      }
    };
  }


}
