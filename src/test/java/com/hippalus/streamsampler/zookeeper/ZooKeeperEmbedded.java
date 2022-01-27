package com.hippalus.streamsampler.zookeeper;

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;

@Slf4j
public class ZooKeeperEmbedded {

  private final TestingServer server;

  public ZooKeeperEmbedded() throws Exception {
    log.debug("Starting embedded ZooKeeper server...");
    this.server = new TestingServer();
    log.debug("Embedded ZooKeeper server at {} uses the temp directory at {}",
        server.getConnectString(), server.getTempDirectory());
  }

  public void stop() throws IOException {
    log.debug("Shutting down embedded ZooKeeper server at {} ...", server.getConnectString());
    server.close();
    log.debug("Shutdown of embedded ZooKeeper server at {} completed", server.getConnectString());
  }

  public String connectString() {
    return server.getConnectString();
  }

  public String hostname() {
    // "server:1:2:3" -> "server:1:2"
    return connectString().substring(0, connectString().lastIndexOf(':'));
  }

}
