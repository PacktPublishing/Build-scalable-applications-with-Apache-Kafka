package com.tomekl007.kafka.consumer;

import com.tomekl007.kafka.AllSpringKafkaTests;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

import static com.tomekl007.kafka.AllSpringKafkaTests.CONSUMER_TEST_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringKafkaReceiverTest {
    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    private Producer<Integer, String> kafkaProducer;

    @Before
    public void setUp() throws Exception {
        // set up the Kafka producer properties
        Map<String, Object> senderProperties =
                KafkaTestUtils.senderProps(AllSpringKafkaTests.embeddedKafka.getBrokersAsString());

        // create a Kafka producer factory
        ProducerFactory<Integer, String> producerFactory =
                new DefaultKafkaProducerFactory<>(senderProperties);

        kafkaProducer = producerFactory.createProducer();

        // wait until the partitions are assigned
        for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry
                .getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer,
                    AllSpringKafkaTests.embeddedKafka.getPartitionsPerTopic());
        }
    }

    @Test
    public void givenConsumer_whenSendMessageToIt_thenShouldReceiveInThePoolLoop() throws Exception {
        //given
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        String message = "Send unique message " + UUID.randomUUID().toString();

        KafkaConsumerWrapper kafkaConsumer = new KafkaConsumerWrapperSyncCommit(
                KafkaTestUtils.consumerProps("group_id" + UUID.randomUUID().toString(), "false", AllSpringKafkaTests.embeddedKafka),
                CONSUMER_TEST_TOPIC
        );

        //when
        executorService.submit(kafkaConsumer::startConsuming);

        for (int i = 0; i < 10; i++) {
            kafkaProducer.send(new ProducerRecord<>(CONSUMER_TEST_TOPIC, message)).get(1, TimeUnit.SECONDS);
        }

        //then
        executorService.awaitTermination(4, TimeUnit.SECONDS);
        executorService.shutdown();
        assertThat(kafkaConsumer.getConsumedEvents().get(0).value()).isEqualTo(message);
    }

    @Test
    public void givenTwoConsumersWithDifferentGroupIds_whenSendMessageToTopic_thenBothShouldReceiveMessages() throws
            InterruptedException, ExecutionException, TimeoutException {
        //given
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        String message = "Send unique message " + UUID.randomUUID().toString();
        KafkaConsumerWrapper kafkaConsumerFirst = new KafkaConsumerWrapperSyncCommit(
                KafkaTestUtils.consumerProps("group_id" + UUID.randomUUID().toString(), "false", AllSpringKafkaTests.embeddedKafka),
                CONSUMER_TEST_TOPIC
        );
        KafkaConsumerWrapper kafkaConsumerSecond = new KafkaConsumerWrapperSyncCommit(
                KafkaTestUtils.consumerProps("group_id" + UUID.randomUUID().toString(), "false", AllSpringKafkaTests.embeddedKafka),
                CONSUMER_TEST_TOPIC
        );

        //when
        executorService.submit(kafkaConsumerFirst::startConsuming);
        executorService.submit(kafkaConsumerSecond::startConsuming);


        for (int i = 0; i < 10; i++) {
            kafkaProducer.send(new ProducerRecord<>(CONSUMER_TEST_TOPIC, message)).get(1, TimeUnit.SECONDS);
        }

        //then
        executorService.awaitTermination(4, TimeUnit.SECONDS);
        executorService.shutdown();
        assertThat(kafkaConsumerFirst.getConsumedEvents().size()).isEqualTo(kafkaConsumerSecond.getConsumedEvents().size());
        assertThat(kafkaConsumerFirst.getConsumedEvents().get(0).value()).isEqualTo(message);
        assertThat(kafkaConsumerSecond.getConsumedEvents().get(0).value()).isEqualTo(message);
    }

    @Test
    public void givenConsumer_whenSendMessageToItAndOffsetOnRebalancingIsLargest_thenShouldConsumeOnlyRecentMessages() throws Exception {
        //given
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        String message = "Send unique message " + UUID.randomUUID().toString();

        KafkaConsumerWrapper kafkaConsumer = new KafkaConsumerWrapperCommitOffsetsOnRebalancing(
                KafkaTestUtils.consumerProps("group_id" + UUID.randomUUID().toString(), "false", AllSpringKafkaTests.embeddedKafka),
                CONSUMER_TEST_TOPIC,
                OffsetResetStrategy.LATEST);

        //when
        sendTenMessages(message);

        executorService.submit(kafkaConsumer::startConsuming);

        sendTenMessages(message);

        //then
        executorService.awaitTermination(4, TimeUnit.SECONDS);
        executorService.shutdown();
        assertThat(kafkaConsumer.getConsumedEvents().size()).isLessThanOrEqualTo(10);
        assertThat(kafkaConsumer.getConsumedEvents().get(0).value()).isEqualTo(message);
    }

    private void sendTenMessages(String message) throws InterruptedException, ExecutionException, TimeoutException {
        for (int i = 0; i < 10; i++) {
            kafkaProducer.send(new ProducerRecord<>(CONSUMER_TEST_TOPIC, message)).get(100, TimeUnit.SECONDS);
        }
    }

    @Test
    public void givenConsumer_whenSendMessageToItAndOffsetOnRebalancingIsSmallest_thenShouldConsumeMessagesFromTheBeginning() throws Exception {
        //given
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        String message = "Send unique message " + UUID.randomUUID().toString();

        KafkaConsumerWrapper kafkaConsumer = new KafkaConsumerWrapperCommitOffsetsOnRebalancing(
                KafkaTestUtils.consumerProps("group_id" + UUID.randomUUID().toString(), "false", AllSpringKafkaTests.embeddedKafka),
                CONSUMER_TEST_TOPIC,
                OffsetResetStrategy.EARLIEST);

        //when
        sendTenMessages(message);
        executorService.submit(kafkaConsumer::startConsuming);
        sendTenMessages(message);

        //then
        executorService.awaitTermination(4, TimeUnit.SECONDS);
        executorService.shutdown();
        assertThat(kafkaConsumer.getConsumedEvents().size()).isGreaterThanOrEqualTo(20);

    }

}
