import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class KafkaProducerBootstrap {

    private static final int NUMBER_OF_MESSAGES = 200;
    private static final String KAFKA_URL = "localhost:9092";
    final private static String TOPIC_NAME = "mymall_feed";

    public static void main(String... args) {

        try {
            KafkaEventProducer kafkaEventProducer = new KafkaEventProducer(KAFKA_URL, TOPIC_NAME);
            sendRandomMessages(kafkaEventProducer);
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    private static void sendRandomMessages(KafkaEventProducer kafkaEventProducer) throws IOException {
        for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            kafkaEventProducer.sendRandomMessage();
            System.out.println("published - " + i);
        }
    }

}
