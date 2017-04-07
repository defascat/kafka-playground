package org.defascat.playground.kafka.topics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 *
 * @author apanasyuk
 */
public class P2PMessageProducerConsumer {
    List<Long> processingTimes = new ArrayList<>();
    public static void main(String[] args) {
        final Properties props = new Properties();
        // props.put("zookeeper.connect", "localhost:2181");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        
        final int maxItems = 10;
        
        final List<DummyProducer> producers = IntStream.range(0, maxItems + 1)
                .mapToObj(ind -> new DummyProducer(ind, props))
                .collect(Collectors.toList());
        
        final Map<String, Integer> topicConfig = new HashMap<>();
        IntStream.range(0, maxItems + 1)
                .mapToObj(ind -> "test-topic-" + ind)
                .forEach(topic -> topicConfig.put(topic, 1));
        ExecutorService consumerPool = Executors.newFixedThreadPool(10);
        IntStream.range(0, maxItems + 1)
                .mapToObj(ind -> new DummyConsumer("test-topic-" + ind, props))
                .forEach(consumer -> consumerPool.execute(() -> consumer.consume()));
        
        ExecutorService producerPool = Executors.newFixedThreadPool(10);
        producers.forEach(producer -> producerPool.execute(() -> producer.sendMessage()));
    }
    
    private static class DummyConsumer {
        private final String name;
        private final KafkaConsumer<String, String> consumer;
        
        private DummyConsumer(String name, Properties properties) {
            properties.put("group.id", name);
            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Arrays.asList(name));
            this.name = name;
        }

        public void consume() {
            System.out.println("Starting consumption: " + name);
            while(true) {
                final ConsumerRecords<String, String> records = consumer.poll(0);
                System.out.println("Polling for " + name + " found " + records.count());
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Received " + name + ": " + record);
                }    
            }
            
        }
    }

    
    private static class DummyProducer extends KafkaProducer<String, String> {
        private final int index;
        public DummyProducer(int index, Properties properties) {
            super(properties);
            this.index = index;
        }
        
        public void sendMessage() {
            final String topicName = "test-topic-" + index;
            System.out.println("Sending message: " + topicName);
            final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "sent", "" + System.currentTimeMillis());
            send(producerRecord);
            System.out.println("Message sent: " + topicName);
        }
    }
}
