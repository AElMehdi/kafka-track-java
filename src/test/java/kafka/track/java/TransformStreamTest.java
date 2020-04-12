package kafka.track.java;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import kafka.track.java.avro.Movie;
import kafka.track.java.avro.RawMovie;
import kafka.track.java.stream.TransformStream;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class TransformStreamTest {

   private static final String TEST_PROPERTIES_FILE = "test.properties";

   @Test
   @Disabled
   void should_convert_movie() {
      var transformStream = new TransformStream();
      Movie movie = transformStream.convertRawMovie(RawMovie.newBuilder()
            .setId(294L)
            .setTitle("Tree of Life::2011")
            .setGenre("drama")
            .build());

      assertThat(movie.getId()).isEqualTo(294L);
      assertThat(movie.getTitle()).isEqualTo("Tree of Life");
      assertThat(movie.getGenre()).isEqualTo("drama");
   }

   @Test
   @Disabled
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

      rawMovies.add(RawMovie.newBuilder().setId(294).setTitle("Die Hard::1988").setGenre("action").build());
      rawMovies.add(RawMovie.newBuilder().setId(354).setTitle("Tree of Life::2011").setGenre("drama").build());
      rawMovies.add(RawMovie.newBuilder().setId(782).setTitle("A Walk in the Clouds::1995").setGenre("romance").build());
      rawMovies.add(RawMovie.newBuilder().setId(128).setTitle("The Big Lebowski::1998").setGenre("comedy").build());

      return rawMovies;
   }

   private List<Movie> movies() {
      List<Movie> movies = new ArrayList<>();

      movies.add(Movie.newBuilder().setId(294).setTitle("Die Hard").setReleaseYear(1988).setGenre("action").build());
      movies.add(Movie.newBuilder().setId(354).setTitle("Tree of Life").setReleaseYear(2011).setGenre("drama").build());
      movies.add(Movie.newBuilder().setId(782).setTitle("A Walk in the Clouds").setReleaseYear(1995).setGenre("romance").build());
      movies.add(Movie.newBuilder().setId(128).setTitle("The Big Lebowski").setReleaseYear(1998).setGenre("comedy").build());

      return movies;
   }
}
