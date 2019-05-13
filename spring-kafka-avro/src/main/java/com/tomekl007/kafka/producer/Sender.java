package com.tomekl007.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import com.tomekl007.avro.User;

public class Sender {

  private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);

  @Value("${topic.avro}")
  private String avroTopic;

  @Autowired
  private KafkaTemplate<String, User> kafkaTemplate;

  public void send(User user) {
    LOGGER.info("sending user='{}'", user.toString());
    kafkaTemplate.send(avroTopic, user);
  }
}
