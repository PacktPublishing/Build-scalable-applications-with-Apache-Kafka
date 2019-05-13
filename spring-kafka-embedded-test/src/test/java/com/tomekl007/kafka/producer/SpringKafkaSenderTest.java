package com.tomekl007.kafka.producer;

import com.tomekl007.kafka.AllSpringKafkaTests;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.tomekl007.kafka.AllSpringKafkaTests.NUMBER_OF_PARTITIONS_PER_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.kafka.test.assertj.KafkaConditions.key;
import static org.springframework.kafka.test.assertj.KafkaConditions.value;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringKafkaSenderTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpringKafkaSenderTest.class);


    private KafkaMessageListenerContainer<Integer, String> container;

    private BlockingQueue<ConsumerRecord<Integer, String>> records;

    @Autowired
    private Sender sender;

    @Before
    public void setUp() throws Exception {
        // set up the Kafka consumer properties
        Map<String, Object> consumerProperties =
                KafkaTestUtils.consumerProps("sender_group", "false",
                        AllSpringKafkaTests.embeddedKafka);

        // create a Kafka consumer factory
        DefaultKafkaConsumerFactory<Integer, String> consumerFactory =
                new DefaultKafkaConsumerFactory<>(consumerProperties);

        // set the topic that needs to be consumed
        ContainerProperties containerProperties =
                new ContainerProperties(AllSpringKafkaTests.SENDER_TOPIC);

        // create a Kafka MessageListenerContainer
        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

        // create a thread safe queue to store the received message
        records = new LinkedBlockingQueue<>();

        // setup a Kafka message listener
        container.setupMessageListener((MessageListener<Integer, String>) record -> {
            LOGGER.debug("test-listener received message='{}'", record.toString());
            records.add(record);
        });

        // start the container and underlying message listener
        container.start();
        // wait until the container has the required number of assigned partitions
        ContainerTestUtils.waitForAssignment(container,
                AllSpringKafkaTests.embeddedKafka.getPartitionsPerTopic());
    }

    @After
    public void tearDown() {
        // stop the container
        container.stop();
    }

    @Test
    public void givenMessage_whenSendBlockingWay_thenConsumerShouldReceiveIt() throws Exception {
        //given
        String content = "User viewed page A";
        Integer userId = 12;

        //when
        sender.sendBlocking(AllSpringKafkaTests.SENDER_TOPIC, content, userId);

        // then
        assertThat(records.poll(10, TimeUnit.SECONDS)).has(value(content)).has(key(userId));
    }

    @Test
    public void givenMessage_whenSendAsyncWay_thenConsumerShouldReceiveIt() throws Exception {
        //given
        String content = "User viewed page B";
        Integer userId = 123;

        //when
        sender.sendAsync(AllSpringKafkaTests.SENDER_TOPIC, content, userId);

        // then
        assertThat(records.poll(10, TimeUnit.SECONDS)).has(value(content))
                .has(key(userId));
    }

    @Test
    public void givenMessage_whenSendForSpecificPartitionKey_thenMessageShouldAlwaysLandInLastPartition() throws Exception {
        //given
        String content = "User viewed page C";
        Integer userId = 777;

        //when
        RecordMetadata recordMetadata = sender.sendBlocking(AllSpringKafkaTests.SENDER_TOPIC, content, userId);

        // then
        assertThat(records.poll(10, TimeUnit.SECONDS)).has(value(content)).has(key(userId));
        assertThat(recordMetadata.partition()).isEqualTo(NUMBER_OF_PARTITIONS_PER_TOPIC - 1);
    }

    @Test
    public void givenMessage_whenSendForOtherThanSpecificPartitionKey_thenMessageShouldAlwaysLandNOTInLastPartition() throws Exception {
        //given
        String content = "User viewed page C";
        Integer userId = new Random().nextInt(100_000);

        //when
        RecordMetadata recordMetadata = sender.sendBlocking(AllSpringKafkaTests.SENDER_TOPIC, content, userId);

        // then
        assertThat(records.poll(10, TimeUnit.SECONDS)).has(value(content)).has(key(userId));
        assertThat(recordMetadata.partition()).isBetween(0, NUMBER_OF_PARTITIONS_PER_TOPIC - 2);
    }

    @Test
    public void givenMessage_whenSendEventForTopicThatNotExists_thenShouldSendMesageToFirstPartitionBecauseKafkaWIllCreateTopicWithOnlyOne() throws Exception {
        //given
        String content = "User viewed page X";
        Integer userId = new Random().nextInt(100_000);
        String topic = "non_existing_topic-that-kafka-will-create-with-one-partition";

        //when
        RecordMetadata recordMetadata = sender.sendBlocking(topic, content, userId);

        // then
        assertThat(recordMetadata.partition()).isEqualTo(0);
    }
}
