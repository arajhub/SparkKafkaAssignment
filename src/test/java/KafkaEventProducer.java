import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;

public class KafkaEventProducer {

    final Producer<Integer, String> producer;
    final EventGenerator eventGenerator;
    private String topicName;



    public KafkaEventProducer(String serverUrl, String topicName) throws URISyntaxException, IOException {
        this.topicName = topicName;
        eventGenerator = new EventGenerator();
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverUrl);
        producerConfig.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "0");
        producerConfig.put("client.id", "basic-producer");
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "0");
        producerConfig.setProperty(ProducerConfig.LINGER_MS_CONFIG, "0");
        producerConfig.put("producer.type", "async");
        producerConfig.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "0");
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        producer = new KafkaProducer<>(producerConfig);

    }

    public void sendRandomMessage() {

        System.out.println("Sending ... ");
        String randomMessage = eventGenerator.getRandomMessage();

        producer.send(new ProducerRecord<>(topicName, randomMessage)
                , (recordMetadata, e) -> {
                    System.out.printf("completed ");
                    System.out.println(randomMessage);
                    if (e == null) {
                        System.out.println("OK");
                    } else {
                        e.printStackTrace();
                    }
                });
    }
}


