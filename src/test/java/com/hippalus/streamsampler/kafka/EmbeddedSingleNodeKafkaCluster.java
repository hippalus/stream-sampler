package com.hippalus.streamsampler.kafka;

import com.hippalus.streamsampler.zookeeper.ZooKeeperEmbedded;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.rules.ExternalResource;
import scala.jdk.CollectionConverters;

@Slf4j
public class EmbeddedSingleNodeKafkaCluster extends ExternalResource {

  private static final int DEFAULT_BROKER_PORT = 9092;

  private ZooKeeperEmbedded zookeeper;
  private KafkaEmbedded broker;
  private final Properties brokerConfig;
  private boolean running;

  public EmbeddedSingleNodeKafkaCluster() {
    this(new Properties());
  }

  public EmbeddedSingleNodeKafkaCluster(final Properties brokerConfig) {
    this.brokerConfig = new Properties();
    this.brokerConfig.putAll(brokerConfig);
  }

  public void start() throws Exception {
    log.debug("Initiating embedded Kafka cluster startup");
    log.debug("Starting a ZooKeeper instance...");
    zookeeper = new ZooKeeperEmbedded();
    log.debug("ZooKeeper instance is running at {}", zookeeper.connectString());

    final Properties effectiveBrokerConfig = effectiveBrokerConfigFrom(brokerConfig, zookeeper);
    log.debug("Starting a Kafka instance on port {} ...", effectiveBrokerConfig.getProperty(KafkaConfig.ListenersProp()));
    broker = new KafkaEmbedded(effectiveBrokerConfig);
    log.debug("Kafka instance is running at {}, connected to ZooKeeper at {}", broker.brokerList(), broker.zookeeperConnect());
    running = true;
  }

  private Properties effectiveBrokerConfigFrom(final Properties brokerConfig, final ZooKeeperEmbedded zookeeper) {
    final Properties effectiveConfig = new Properties();
    effectiveConfig.putAll(brokerConfig);
    effectiveConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), zookeeper.connectString());
    effectiveConfig.put(KafkaConfig$.MODULE$.ZkSessionTimeoutMsProp(), 30 * 1000);
    effectiveConfig.put(KafkaConfig.ListenersProp(), String.format("PLAINTEXT://127.0.0.1:%s", DEFAULT_BROKER_PORT));
    effectiveConfig.put(KafkaConfig$.MODULE$.ZkConnectionTimeoutMsProp(), 60 * 1000);
    effectiveConfig.put(KafkaConfig$.MODULE$.DeleteTopicEnableProp(), true);
    effectiveConfig.put(KafkaConfig$.MODULE$.LogCleanerDedupeBufferSizeProp(), 2 * 1024 * 1024L);
    effectiveConfig.put(KafkaConfig$.MODULE$.GroupMinSessionTimeoutMsProp(), 0);
    effectiveConfig.put(KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), (short) 1);
    effectiveConfig.put(KafkaConfig$.MODULE$.OffsetsTopicPartitionsProp(), 1);
    effectiveConfig.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);
    return effectiveConfig;
  }

  @Override
  protected void before() throws Exception {
    start();
  }

  @Override
  protected void after() {
    stop();
  }

  public void stop() {
    log.info("Stopping Confluent");
    try {
      if (broker != null) {
        broker.stop();
      }
      try {
        if (zookeeper != null) {
          zookeeper.stop();
        }
      } catch (final IOException fatal) {
        throw new RuntimeException(fatal);
      }
    } finally {
      running = false;
    }
    log.info("Kafka Stopped");
  }

  public String bootstrapServers() {
    return broker.brokerList();
  }


  public void createTopic(final String topic) throws InterruptedException {
    createTopic(topic, 1, (short) 1, Collections.emptyMap());
  }


  public void createTopic(final String topic, final int partitions, final short replication) throws InterruptedException {
    createTopic(topic, partitions, replication, Collections.emptyMap());
  }

  public void createTopic(final String topic,
      final int partitions,
      final short replication,
      final Map<String, String> topicConfig) throws InterruptedException {
    createTopic(60000L, topic, partitions, replication, topicConfig);
  }

  public void createTopic(final long timeoutMs,
      final String topic,
      final int partitions,
      final short replication,
      final Map<String, String> topicConfig) throws InterruptedException {
    broker.createTopic(topic, partitions, replication, topicConfig);

    if (timeoutMs > 0) {
      TestUtils.waitForCondition(new TopicCreatedCondition(topic), timeoutMs,
          "Topics not created after " + timeoutMs + " milli seconds.");
    }
  }

  public void deleteTopicsAndWait(final long timeoutMs, final String... topics) throws InterruptedException {
    for (final String topic : topics) {
      try {
        broker.deleteTopic(topic);
      } catch (final UnknownTopicOrPartitionException expected) {
        // indicates (idempotent) success
      }
    }
    if (timeoutMs > 0) {
      TestUtils.waitForCondition(new TopicsDeletedCondition(topics), timeoutMs,
          "Topics not deleted after " + timeoutMs + " milli seconds.");
    }
  }

  public boolean isRunning() {
    return running;
  }

  private final class TopicsDeletedCondition implements TestCondition {

    final Set<String> deletedTopics = new HashSet<>();

    private TopicsDeletedCondition(final String... topics) {
      Collections.addAll(deletedTopics, topics);
    }

    @Override
    public boolean conditionMet() {
      final Set<String> allTopicsFromZk = new HashSet<>(
          CollectionConverters.SetHasAsJava(broker.kafkaServer().zkClient().getAllTopicsInCluster(false)).asJava());

      final Set<String> allTopicsFromBrokerCache = new HashSet<>(
          CollectionConverters.SeqHasAsJava(broker.kafkaServer().metadataCache().getAllTopics().toSeq()).asJava());

      return !allTopicsFromZk.removeAll(deletedTopics) && !allTopicsFromBrokerCache.removeAll(deletedTopics);
    }
  }

  private final class TopicCreatedCondition implements TestCondition {

    final String createdTopic;

    private TopicCreatedCondition(final String topic) {
      createdTopic = topic;
    }

    @Override
    public boolean conditionMet() {
      return broker.kafkaServer().zkClient().getAllTopicsInCluster(false).contains(createdTopic) &&
          broker.kafkaServer().metadataCache().contains(createdTopic);
    }
  }

}
