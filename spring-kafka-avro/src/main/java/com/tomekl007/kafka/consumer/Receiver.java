package com.tomekl007.kafka.consumer;

import java.util.concurrent.CountDownLatch;

import com.tomekl007.avro.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;



public class Receiver {

  private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

  private CountDownLatch latch = new CountDownLatch(1);

  public CountDownLatch getLatch() {
    return latch;
  }

  @KafkaListener(topics = "${topic.avro}")
  public void receive(User user) {
    LOGGER.info("received user='{}'", user.toString());
    latch.countDown();
  }
}
