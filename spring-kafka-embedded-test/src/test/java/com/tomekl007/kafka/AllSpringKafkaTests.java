package com.tomekl007.kafka;

import com.tomekl007.kafka.consumer.SpringKafkaReceiverTest;
import com.tomekl007.kafka.producer.SpringKafkaSenderTest;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;

@RunWith(Suite.class)
@SuiteClasses({SpringKafkaApplicationTest.class, SpringKafkaSenderTest.class,
        SpringKafkaReceiverTest.class})
public class AllSpringKafkaTests {

    private static final Logger LOGGER = LoggerFactory.getLogger(AllSpringKafkaTests.class);

    public static final String SENDER_TOPIC = "sender.t";
    public static final String RECEIVER_TOPIC = "receiver.t";
    public static final String CONSUMER_TEST_TOPIC = "consumer_test_topic";
    public static final Integer NUMBER_OF_PARTITIONS_PER_TOPIC = 4;

    @ClassRule
    public static KafkaEmbedded embeddedKafka =
            new KafkaEmbedded(
                    1,
                    true,
                    NUMBER_OF_PARTITIONS_PER_TOPIC,
                    SENDER_TOPIC,
                    RECEIVER_TOPIC,
                    CONSUMER_TEST_TOPIC
            );

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        String kafkaBootstrapServers = embeddedKafka.getBrokersAsString();

        LOGGER.debug("kafkaServers='{}'", kafkaBootstrapServers);
        // override the property in application.properties
        System.setProperty("kafka.bootstrap-servers", kafkaBootstrapServers);
    }
}
