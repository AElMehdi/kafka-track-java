package kafka.track.stream;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import kafka.track.java.avro.Movie;
import kafka.track.java.avro.RawMovie;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static java.lang.Integer.parseInt;

public class TransformStream {

    public Movie convertRawMovie(RawMovie rawMovie) {
        String[] splitTitle = rawMovie.getTitle().split("::");

        String title = splitTitle[0];
        String releaseYear = splitTitle[1];

        return new Movie(rawMovie.getId(), title, parseInt(releaseYear), rawMovie.getGenre());
    }

    public Topology buildTopology(Properties envProps) {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final String inputTopicName = envProps.getProperty("input.topic.name");

        KStream<String, RawMovie> rawMovies = streamsBuilder.stream(inputTopicName);
        KStream<Long, Movie> movies = rawMovies.map(
                (key, rawMovie) -> new KeyValue<>(rawMovie.getId(), convertRawMovie(rawMovie)));

        movies.to("movies", Produced.with(Serdes.Long(), movieAvroSerde(envProps)));

        return streamsBuilder.build();
    }

    private SpecificAvroSerde<Movie> movieAvroSerde(Properties envProps) {
        SpecificAvroSerde<Movie> movieAvroSerde = new SpecificAvroSerde<>();

        HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));

        return movieAvroSerde;
    }

    public Properties loadEnvProperties(String fileName) throws IOException {
        Properties envProps = new Properties();
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource(fileName).getFile());
        FileInputStream inputFile = new FileInputStream(file);
        envProps.load(inputFile);

        inputFile.close();
        return envProps;
    }

    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));

        return props;
    }

   private void createTopics(Properties envProps) {
       Map<String, Object> config = new HashMap<>();
       config.put("bootstrap.servers", envProps.getProperty("bootstrap.servers"));
       AdminClient client = AdminClient.create(config);

       List<NewTopic> topics = new ArrayList<>();

       topics.add(new NewTopic(
               envProps.getProperty("input.topic.name"),
               Integer.parseInt(envProps.getProperty("input.topic.partitions")),
               Short.parseShort(envProps.getProperty("input.topic.replication.factor"))));

       topics.add(new NewTopic(
               envProps.getProperty("output.topic.name"),
               Integer.parseInt(envProps.getProperty("output.topic.partitions")),
               Short.parseShort(envProps.getProperty("output.topic.replication.factor"))));

       client.createTopics(topics);
       client.close();
   }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: the path to an environment configuration file.");
        }

        TransformStream transformStream = new TransformStream();
        Properties envProps = transformStream.loadEnvProperties(args[0]);
        Properties streamProps = transformStream.buildStreamsProperties(envProps);
        Topology topology = transformStream.buildTopology(envProps);

        transformStream.createTopics(envProps);

        final KafkaStreams streams = new KafkaStreams(topology, streamProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
