package com.hippalus.streamsampler;

import com.hippalus.streamsampler.kafka.EmbeddedSingleNodeKafkaCluster;
import java.util.Properties;
import kafka.server.KafkaConfig;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import redis.embedded.RedisServer;

@Slf4j
@SpringBootTest
@DirtiesContext
public abstract class AbstractIntegrationTest {

  public static RedisServer REDIS_SERVER;

  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster(
      new Properties() {
        {
          //Transactions need durability so the defaults require multiple nodes.
          //For testing purposes set transactions to work with a single kafka broker.
          put(KafkaConfig.TransactionsTopicReplicationFactorProp(), "1");
          put(KafkaConfig.TransactionsTopicMinISRProp(), "1");
          put(KafkaConfig.TransactionsTopicPartitionsProp(), "1");
        }
      });

  @SneakyThrows
  @BeforeAll
  public static void startCluster() {
    stopCluster();
    CLUSTER.start();
    REDIS_SERVER = RedisServer.builder().port(6379).build();
    REDIS_SERVER.start();
  }

  @AfterAll
  public static void stopCluster() {
    log.info("stopping cluster");
    if (CLUSTER.isRunning()) {
      CLUSTER.stop();
    }
    if (REDIS_SERVER != null && REDIS_SERVER.isActive()) {
      REDIS_SERVER.stop();
    }
  }


  protected static Properties producerConfig(final EmbeddedSingleNodeKafkaCluster cluster) {
    final Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    return producerConfig;
  }
}
