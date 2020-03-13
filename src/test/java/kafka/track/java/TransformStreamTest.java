package kafka.track.java;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.jupiter.api.Test;

public class TransformStreamTest {

   private static final String TEST_PROPERTIES_FILE = "test.properties";

   @Test
   void should_convert_movie() {
      var transformStream = new TransformStream();
      Movie movie = transformStream.convertRawMovie(new RawMovie.Builder()
            .id(294L)
            .title("Tree of Life::2011")
            .genre("drama")
            .build());

      assertThat(movie.getId()).isEqualTo(294L);
      assertThat(movie.getTitle()).isEqualTo("Tree of Life");
      assertThat(movie.getGenre()).isEqualTo("drama");
   }

   @Test
   void should_transform_a_kafka_movies_stream() throws IOException {
      TransformStream transformStream = new TransformStream();


      Properties envProps = transformStream.loadEnvProperties(TEST_PROPERTIES_FILE);
      Topology topology = transformStream.buildTopology(envProps);

      String inputTopicName = envProps.getProperty("input.topic.name");
      String outputTopicName = envProps.getProperty("output.topic.name");

      Properties streamConfig = transformStream.buildStreamsProperties(envProps);
      TopologyTestDriver testDriver = new TopologyTestDriver(topology, streamConfig);

      Serializer<String> keySerializer = Serdes.String().serializer();
      SpecificAvroSerializer<RawMovie> valueSerializer = makeSerializer(envProps);

      var inputFactory = new ConsumerRecordFactory<>(keySerializer, valueSerializer);

      for (RawMovie rawMovie : rawMovies()) {
         // Need to know why they were passing a rawMovie object!
         testDriver.pipeInput(inputFactory.create(inputTopicName, rawMovie.getTitle(), rawMovie));
      }

      List<Movie> expected = movies();

      Deserializer<String> keyDeserializer = Serdes.String().deserializer();
      SpecificAvroDeserializer valueDeserializer = makeDeserializer(envProps);
      List<Movie> transformed = readOutputTopic(testDriver, outputTopicName, keyDeserializer, valueDeserializer);

      assertThat(transformed.size()).isEqualTo(expected.size());
   }

   private SpecificAvroSerializer<RawMovie> makeSerializer(Properties envProps) {
      SpecificAvroSerializer<RawMovie> serializer = new SpecificAvroSerializer<>();

      Map<String, String> config = new HashMap<>();
      config.put("schema.registry.url", envProps.getProperty("schema.registry.url"));
      serializer.configure(config, false);

      return serializer;
   }

   private SpecificAvroDeserializer makeDeserializer(Properties envProps) {
      SpecificAvroDeserializer<RawMovie> deserializer = new SpecificAvroDeserializer<>();

      Map<String, String> config = new HashMap<>();
      config.put("schema.registry.url", envProps.getProperty("schema.registry.url"));
      deserializer.configure(config, false);

      return deserializer;
   }

   private List<Movie> readOutputTopic(TopologyTestDriver testDriver, String outputTopic, Deserializer<String> keyDeserializer, SpecificAvroDeserializer valueDeserializer) {
      List<Movie> result = new ArrayList<>();

      while (true) {
         ProducerRecord<String, Movie> record = testDriver.readOutput(outputTopic, keyDeserializer, valueDeserializer);

         if (record != null) {
            result.add(record.value());
         } else {
            break;
         }
      }


      return result;
   }

   private List<RawMovie> rawMovies() {
      List<RawMovie> rawMovies = new ArrayList<>();

      rawMovies.add(new RawMovie.Builder().id(294).title("Die Hard::1988").genre("action").build());
      rawMovies.add(new RawMovie.Builder().id(354).title("Tree of Life::2011").genre("drama").build());
      rawMovies.add(new RawMovie.Builder().id(782).title("A Walk in the Clouds::1995").genre("romance").build());
      rawMovies.add(new RawMovie.Builder().id(128).title("The Big Lebowski::1998").genre("comedy").build());

      return rawMovies;
   }

   private List<Movie> movies() {
      List<Movie> movies = new ArrayList<>();

      movies.add(new Movie(294, "Die Hard", 1988, "action"));
      movies.add(new Movie(354, "Tree of Life", 2011, "drama"));
      movies.add(new Movie(782, "A Walk in the Clouds", 1995, "romance"));
      movies.add(new Movie(128, "The Big Lebowski", 1998, "comedy"));

      return movies;
   }
}
