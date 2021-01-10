import org.apache.commons.lang3.text.StrSubstitutor;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.concurrent.ThreadLocalRandom;

public class EventGenerator {

    private static String EVENT_TEMPLATE_NAME = "event.json.template";
    private final String eventTemplate;
    private static Location storeLocation = new Location("Bangalore", 12.9716, 77.5946);

    public EventGenerator() throws IOException, URISyntaxException {
        Path path = Paths.get(getClass().getClassLoader()
                .getResource(EVENT_TEMPLATE_NAME).toURI());
        Stream<String> lines = Files.lines(path);
        eventTemplate = lines.collect(Collectors.joining("\n"));
        lines.close();
    }

    public String getRandomMessage() {
        Map<String,String> params = new HashMap<>();
        params.put("EVENT_TIME", String.valueOf(System.currentTimeMillis()));
        Location randonLocation = Location.getLocationInLatLngRad(5000, storeLocation);
        params.put("CUST_LAT", String.valueOf(randonLocation.getLatitude()));
        params.put("CUST_LON",String.valueOf(randonLocation.getLongitude()));
        params.put("CUST_ID",String.valueOf(ThreadLocalRandom.current().nextInt(1, 200 + 1)));
        params.put("DEVICE_ID",UUID.randomUUID().toString());
        StrSubstitutor substitutor = new StrSubstitutor(params);
        return substitutor.replace(eventTemplate);
    }

}
