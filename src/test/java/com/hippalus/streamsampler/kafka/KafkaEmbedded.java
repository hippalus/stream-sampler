package com.hippalus.streamsampler.kafka;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import kafka.cluster.EndPoint;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.junit.rules.TemporaryFolder;

@Slf4j
public class KafkaEmbedded {

  private static final String DEFAULT_ZK_CONNECT = "127.0.0.1:2181";

  private final Properties effectiveConfig;
  private final File logDir;
  private final TemporaryFolder tmpFolder;
  private final KafkaServer kafka;

  public KafkaEmbedded(final Properties config) throws IOException {
    tmpFolder = new TemporaryFolder();
    tmpFolder.create();
    logDir = tmpFolder.newFolder();
    effectiveConfig = effectiveConfigFrom(config);
    final boolean loggingEnabled = true;

    final KafkaConfig kafkaConfig = new KafkaConfig(effectiveConfig, loggingEnabled);
    log.debug("Starting embedded Kafka broker (with log.dirs={} and ZK ensemble at {}) ...",
        logDir, zookeeperConnect());
    kafka = TestUtils.createServer(kafkaConfig, Time.SYSTEM);
    log.debug("Startup of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
        brokerList(), zookeeperConnect());
  }

  private Properties effectiveConfigFrom(final Properties initialConfig) {
    final Properties effectiveConfig = new Properties();
    effectiveConfig.put(KafkaConfig$.MODULE$.BrokerIdProp(), 0);
    effectiveConfig.put(KafkaConfig.ListenersProp(), "PLAINTEXT://127.0.0.1:9092");
    effectiveConfig.put(KafkaConfig$.MODULE$.NumPartitionsProp(), 1);
    effectiveConfig.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);
    effectiveConfig.put(KafkaConfig$.MODULE$.MessageMaxBytesProp(), 1000000);
    effectiveConfig.put(KafkaConfig$.MODULE$.ControlledShutdownEnableProp(), true);

    effectiveConfig.putAll(initialConfig);
    effectiveConfig.setProperty(KafkaConfig$.MODULE$.LogDirProp(), logDir.getAbsolutePath());
    return effectiveConfig;
  }

  public String brokerList() {
    final EndPoint endPoint = kafka.advertisedListeners().head();
    final String hostname = endPoint.host() == null ? "" : endPoint.host();

    return String.join(":", hostname, Integer.toString(
        kafka.boundPort(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    ));
  }

  public String zookeeperConnect() {
    return effectiveConfig.getProperty("zookeeper.connect", DEFAULT_ZK_CONNECT);
  }

  public void stop() {
    log.debug("Shutting down embedded Kafka broker at {} (with ZK ensemble at {}) ...",
        brokerList(), zookeeperConnect());
    kafka.shutdown();
    kafka.awaitShutdown();
    log.debug("Removing temp folder {} with logs.dir at {} ...", tmpFolder, logDir);
    log.debug("Shutdown of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
        brokerList(), zookeeperConnect());
  }

  public void createTopic(final String topic) {
    createTopic(topic, 1, (short) 1, Collections.emptyMap());
  }

  public void createTopic(final String topic, final int partitions, final short replication) {
    createTopic(topic, partitions, replication, Collections.emptyMap());
  }

  public void createTopic(final String topic,
      final int partitions,
      final short replication,
      final Map<String, String> topicConfig) {
    log.debug("Creating topic { name: {}, partitions: {}, replication: {}, config: {} }",
        topic, partitions, replication, topicConfig);

    final Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList());

    try (final AdminClient adminClient = AdminClient.create(properties)) {
      final NewTopic newTopic = new NewTopic(topic, partitions, replication);
      newTopic.configs(topicConfig);
      adminClient.createTopics(Collections.singleton(newTopic)).all().get();
    } catch (final InterruptedException | ExecutionException fatal) {
      throw new RuntimeException(fatal);
    }

  }

  public void deleteTopic(final String topic) {
    log.debug("Deleting topic {}", topic);
    final Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList());

    try (final AdminClient adminClient = AdminClient.create(properties)) {
      adminClient.deleteTopics(Collections.singleton(topic)).all().get();
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    } catch (final ExecutionException e) {
      if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
        throw new RuntimeException(e);
      }
    }
  }

  KafkaServer kafkaServer() {
    return kafka;
  }
}
