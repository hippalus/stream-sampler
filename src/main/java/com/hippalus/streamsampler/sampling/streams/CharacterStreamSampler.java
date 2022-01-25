package com.hippalus.streamsampler.sampling.streams;

import com.hippalus.streamsampler.sampling.RandomReservoirSampler;
import com.hippalus.streamsampler.sampling.RandomSampler;
import com.hippalus.streamsampler.sampling.Sample;
import com.hippalus.streamsampler.sampling.SamplePriorityQueue;
import com.hippalus.streamsampler.sampling.SamplePriorityQueueRedisAdapter;
import com.hippalus.streamsampler.sampling.SamplerLock;
import com.hippalus.streamsampler.sampling.SamplerLockRedisAdapter;
import com.hippalus.streamsampler.sampling.streams.config.StreamSamplerConfig;
import java.util.Collection;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class CharacterStreamSampler implements StreamSampler<Character> {

  private final StreamSamplerConfig config;
  private final RedissonClient redissonClient;
  private KafkaStreams characterKStream;
  private RandomSampler<Character> randomSampler;

  @Override
  public void start(final int sampleSize) {
    initRandomSampler(sampleSize);
    final Topology samplerTopology = buildSamplerTopology(config);
    this.characterKStream = new KafkaStreams(samplerTopology, config.defaultStreamConfig());
    this.characterKStream.start();
    addShutdownHookAndBlock(this);
  }

  private void initRandomSampler(final int sampleSize) {
    final SamplePriorityQueue<Sample<Character>> sampleQueue = new SamplePriorityQueueRedisAdapter<>(redissonClient);
    final SamplerLock samplerLock = new SamplerLockRedisAdapter(redissonClient);
    this.randomSampler = new RandomReservoirSampler<>(sampleSize, sampleQueue, samplerLock);
  }

  private Topology buildSamplerTopology(final StreamSamplerConfig config) {
    final StreamsBuilder builder = new StreamsBuilder();
    builder
        .stream(config.getInboundTopic(), config.inboundSerDes())
        .mapValues((key, value) -> value.chars().mapToObj(this::castToChar))
        .filter((key, value) -> Objects.nonNull(value))
        .flatMap((key, characterStream) -> characterStream.map(character -> KeyValue.pair(key, character)).toList())
        .foreach((key, value) -> randomSampler.feed(value));
    return builder.build();
  }

  private Character castToChar(int i) {
    try {
      return (char) i;
    } catch (Exception exception) {
      log.error("Exception has been occurred while casting {} to char. {}", i, exception.getMessage());
    }
    return null;
  }

  @Override
  public void stop() {
    if (characterKStream != null) {
      characterKStream.close();
    }
  }

  @Override
  public Collection<Character> currentSample() {
    assert randomSampler != null;
    return randomSampler.sample();
  }
}
