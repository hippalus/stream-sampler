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
    final Topology samplerTopology = buildSamplerTopology();
    this.characterKStream = new KafkaStreams(samplerTopology, config.defaultStreamConfig());
    cleanLocalState();// TODO: conditional clean by application profile. Don't use in production mode
    this.characterKStream.start();
    addShutdownHookAndBlock(this);
  }

  private void initRandomSampler(final int sampleSize) {
    final SamplePriorityQueue<Sample<Character>> sampleQueue = new SamplePriorityQueueRedisAdapter<>(redissonClient);
    final SamplerLock samplerLock = new SamplerLockRedisAdapter(redissonClient);
    this.randomSampler = new RandomReservoirSampler<>(sampleSize, sampleQueue, samplerLock);
  }

  private Topology buildSamplerTopology() {
    final StreamsBuilder builder = new StreamsBuilder();
    builder
        .stream(config.getInboundTopic(), config.inboundSerDes())
        .filter((key, value) -> Objects.nonNull(value))
        .filter((key, value) -> !value.isEmpty())
        .mapValues((readOnlyKey, value) -> value.charAt(0))// we assumed that each incoming String is a Character
        .foreach((key, value) -> randomSampler.feed(value)); //distributed stateless
    return builder.build();
  }

  @Override
  public void stop() {
    if (characterKStream != null) {
      characterKStream.close();
    }
  }

  @Override
  public void cleanLocalState() {
    if (characterKStream != null) {
      characterKStream.cleanUp();
    }
    randomSampler.clear();
  }

  @Override
  public Collection<Character> currentSample() {
    assert randomSampler != null;
    return randomSampler.sample();
  }
}
