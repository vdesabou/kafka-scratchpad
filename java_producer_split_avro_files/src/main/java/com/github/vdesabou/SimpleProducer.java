package com.github.vdesabou;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import com.github.vdesabou.SearchResult;
import com.github.vdesabou.SearchResultType;
import com.github.javafaker.Faker;

// the goal is to test this: https://stackoverflow.com/questions/21539113/can-i-split-an-apache-avro-schema-across-multiple-files

public class SimpleProducer {

    private static final String TOPIC = "search-result-avro";

    public static void main(String[] args) throws InterruptedException {

        // confluent local start
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        // Schema Registry specific settings
        props.put("schema.registry.url", "http://127.0.0.1:8081");

        System.out.println("Sending data to `search-result-avro` topic. Properties: " + props.toString());

        Faker faker = new Faker();

        String key = "alice";
        try (Producer<String, SearchResult> producer = new KafkaProducer<>(props)) {
            while (true) {

                SearchResult searchresult = SearchResult.newBuilder()
                .setKeyWord(faker.name().username())
                .setType(SearchResultType.A)
                .setSearchEngine(faker.name().name())
                .setPosition(1)
                .setUserAction(UserAction.C)
                .build();

                ProducerRecord<String, SearchResult> record = new ProducerRecord<>(TOPIC, key, searchresult);
                System.out.println("Sending " + record.key() + " " + record.value());
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", metadata.topic(), metadata.partition(), metadata.offset());
                        } else {
                            exception.printStackTrace();
                        }
                    }
                });
                producer.flush();
                TimeUnit.MILLISECONDS.sleep(5000);
            }
        }
    }
}
