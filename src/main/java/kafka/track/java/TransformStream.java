package kafka.track.java;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import static java.lang.Integer.parseInt;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

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
}
