package com.tomekl007.kafka.consumer;


import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class KafkaConsumerWrapperSyncCommit implements KafkaConsumerWrapper {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerWrapperSyncCommit.class);
    private KafkaConsumer<Integer, String> consumer;
    public List<ConsumerRecord<Integer, String>> consumedMessages = new LinkedList<>();

    public KafkaConsumerWrapperSyncCommit(Map<String, Object> properties, String topic) {
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public void startConsuming() {
        try {
            while (true) {
                ConsumerRecords<Integer, String> records = consumer.poll(100);
                for (ConsumerRecord<Integer, String> record : records) {
                    LOGGER.debug("topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    logicProcessing(record);//important to be before commit offset
                    try {
                        consumer.commitSync();
                    } catch (CommitFailedException e) {
                        LOGGER.error("commit failed", e);
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

    @Override
    public List<ConsumerRecord<Integer, String>> getConsumedEvents() {
        return consumedMessages;
    }

    private void logicProcessing(ConsumerRecord<Integer, String> record) {
        consumedMessages.add(record);
    }
}
