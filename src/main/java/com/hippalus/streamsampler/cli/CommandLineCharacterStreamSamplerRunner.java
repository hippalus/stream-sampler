package com.hippalus.streamsampler.cli;

import com.hippalus.streamsampler.sampling.Utils;
import com.hippalus.streamsampler.sampling.streams.StreamSampler;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class CommandLineCharacterStreamSamplerRunner implements CommandLineRunner {

  @Value("${application.stream-sampler.inbound-topic}")
  private String characterTopicName;
  private final HelpFormatter helpFormatter = new HelpFormatter();
  private final DefaultParser defaultParser = new DefaultParser();
  private final StreamSampler<Character> characterStreamSampler;
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final Environment environment;

  @Override
  public void run(String... args) throws Exception {
    final Set<String> activeProfiles = Arrays.stream(environment.getActiveProfiles()).collect(Collectors.toSet());
    if (activeProfiles.contains("cli") && !activeProfiles.contains("dev")) {
      runOnCliProfile(args);
    }
    if (!activeProfiles.contains("cli") && activeProfiles.contains("dev")) {
      runOnDevProfile();
    }
  }

  private void runOnDevProfile() {
    generateRandomDataAndFeedCharTopic(10000L);
    new Thread(() -> characterStreamSampler.start(20)).start();
    new Thread(this::trackSample).start();
  }

  private void runOnCliProfile(String[] args) {
    final Options options = prepareOptions();
    try {
      CommandLine cli = defaultParser.parse(options, args);
      if (cli.hasOption("help")) {
        printHelp(options);
        System.exit(0);
      }

      if (cli.hasOption("generate")) {
        final Long generateCount = (Long) cli.getParsedOptionValue("generate");
        new Thread(() -> generateRandomDataAndFeedCharTopic(generateCount)).start();
      } else {
        new Thread(this::readStdInAndFeedCharTopic).start();
      }

      final int sampleSize = ((Long) cli.getParsedOptionValue("size")).intValue();
      new Thread(() -> characterStreamSampler.start(sampleSize)).start();
      trackSample();
    } catch (ParseException e) {
      printHelp(options);
      System.exit(1);
    }
  }

  @SuppressWarnings("InfiniteLoopStatement")
  //FIXME:
  private void trackSample() {
    while (true) {
      try {
        final String sample = getSample();
        log.info("Current Sample {}", sample);
        Utils.sleep(10000);
      } catch (Exception ignored) {
        //ignored
      }
    }
  }

  private String getSample() {
    return characterStreamSampler.currentSample()
        .stream()
        .map(Object::toString)
        .collect(Collectors.joining());
  }

  private void generateRandomDataAndFeedCharTopic(Long generateCount) {
    RandomStringUtils.randomAlphabetic(generateCount.intValue())
        .chars()
        .mapToObj(Utils::castToChar)
        .filter(Objects::nonNull)
        .forEach(character -> kafkaTemplate.send(new ProducerRecord<>(characterTopicName, null, character.toString())));
  }


  private Options prepareOptions() {
    Options opts = new Options();
    opts.addOption(sizeOption());
    opts.addOption(generatorOption());
    opts.addOption(helpOption());
    return opts;
  }

  private Option helpOption() {
    return Option.builder()
        .longOpt("help")
        .build();
  }

  private Option generatorOption() {
    return Option.builder("g")
        .hasArg()
        .type(Number.class)
        .desc("Size of random input")
        .longOpt("generate")
        .argName("INPUT_SIZE")
        .build();
  }

  private Option sizeOption() {
    return Option.builder("n")
        .required()
        .longOpt("size")
        .hasArg()
        .type(Number.class)
        .desc("Sample size")
        .argName("SAMPLE_SIZE")
        .build();
  }

  private void readStdInAndFeedCharTopic() {
    try (final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in))) {
      bufferedReader.lines()
          .<Character>mapMulti((s, consumer) -> s.chars()
              .mapToObj(Utils::castToChar)
              .filter(Objects::nonNull)
              .forEach(consumer))
          .map(Object::toString)
          .forEach(charStr -> kafkaTemplate.send(new ProducerRecord<>(characterTopicName, null, charStr)));
    } catch (IOException e) {
      log.error("Exception has been occurred while reading stdin", e);
    }
  }

  private void printHelp(Options opts) {
    helpFormatter.printHelp(
        """
            cat file.txt | ./stream-sampler.sh -n SAMPLE_SIZE
            """,
        """
            Creates a random representative sample of length SAMPLE_SIZE out of the input.
            Input is either STDIN, or randomly generated within application.
            """,
        opts,
        ""
    );
  }

}
