package com.luisjrz96.kafka.stream.uppercasesample;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Assertions;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
public class UppercaseSampleApplicationTests {

    private static final String[] topics = {"input-uppercase-topic", "output-uppercase-topic"};

    @Container
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"));

    @Value("${spring.cloud.stream.bindings.process-in-0.destination}")
    private String inputUppercaseTopic;

    @Value("${spring.cloud.stream.bindings.process-out-0.destination}")
    private String outputUpperCaseTopic;

    @BeforeAll
    public static void setup() {
        kafka.start();
        var newTopics = Arrays.stream(topics).map(topic -> new NewTopic(topic,1 , (short)1))
                .collect(Collectors.toList());
        try(var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, String.format("%s:%d",
                "localhost", kafka.getFirstMappedPort())))) {
            System.setProperty("spring.cloud.stream.kafka.binder.brokers", kafka.getBootstrapServers());
            admin.createTopics(newTopics);
        }
    }

    @AfterAll
    public static void finish() {
        kafka.close();
    }

    @Test
    public void testUpperCaseStreamWithNonEmptyKeyAndValue() {
        testUpperCaseStream("hello", "world", "HELLO", "WORLD");
    }


    @Test
    public void testUpperCaseStreamWithNullKey() {
        testUpperCaseStream(null, "world", UppercaseSampleApplication.DEFAULT_KEY, "WORLD");
    }

    @Test
    public void testUpperCaseStreamWithNullValue() {
        testUpperCaseStream("hello", null, "HELLO", UppercaseSampleApplication.DEFAULT_VALUE);
    }

    public void testUpperCaseStream(String inputKey, String inputValue, String outputKey, String outputValue) {
        try(KafkaProducer<String, String> producer = new KafkaProducer<String, String>(
                ImmutableMap.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                                ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()),
                new StringSerializer(),
                new StringSerializer()
        );
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(
              ImmutableMap.of(
                      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                      ConsumerConfig.GROUP_ID_CONFIG, "demo",
                      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
              ), new StringDeserializer(), new StringDeserializer()
            );
        ){
            consumer.subscribe(Arrays.asList(outputUpperCaseTopic));
            producer.send(new ProducerRecord<>(inputUppercaseTopic, inputKey, inputValue));
            producer.flush();
            Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, ()-> {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if(records.isEmpty()) {
                    return false;
                }
                Assertions.assertThat(records)
                        .hasSize(1)
                        .extracting(ConsumerRecord::topic, ConsumerRecord::key, ConsumerRecord::value)
                        .containsExactly(Tuple.tuple(outputUpperCaseTopic, outputKey, outputValue));

                return true;
            });
        }
    }
}