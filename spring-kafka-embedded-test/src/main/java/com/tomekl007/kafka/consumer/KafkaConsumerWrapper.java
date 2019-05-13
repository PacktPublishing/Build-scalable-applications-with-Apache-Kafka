package com.tomekl007.kafka.consumer;


import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

public interface KafkaConsumerWrapper {
    void startConsuming();

    List<ConsumerRecord<Integer, String>> getConsumedEvents();
}
