package com.hippalus.streamsampler.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedissonConfig {

  @Value("${redis.addresses}")
  private String redisAddress;

  @Bean
  public RedissonClient redissonClient() {
    final Config config = new Config();
    config.useSingleServer().setAddress(redisAddress);
    config.setCodec(JsonJacksonCodec.INSTANCE);
    return Redisson.create(config);
  }
}
