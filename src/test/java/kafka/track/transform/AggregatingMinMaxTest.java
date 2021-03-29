package kafka.track.transform;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import kafka.track.java.avro.MovieTicketSales;
import kafka.track.java.avro.YearlyMovieFigures;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class AggregatingMinMaxTest {

    private final static String TEST_CONFIG_FILE = "min-max-app/test.properties";

    private static SpecificAvroSerde<MovieTicketSales> makeMovieTicketSalesSerializer(
            Properties envProps, SchemaRegistryClient srClient) {

        SpecificAvroSerde<MovieTicketSales> serde = new SpecificAvroSerde<>(srClient);
        serde.configure(Collections.singletonMap(
                "schema.registry.url", envProps.getProperty("schema.registry.url")), false);
        return serde;
    }

    private static SpecificAvroSerde<YearlyMovieFigures> makeYearlyMovieFiguresSerializer(
            Properties envProps, SchemaRegistryClient srClient) {

        SpecificAvroSerde<YearlyMovieFigures> serde = new SpecificAvroSerde<>(srClient);
        serde.configure(Collections.singletonMap(
                "schema.registry.url", envProps.getProperty("schema.registry.url")), false);
        return serde;
    }

    @Test
    void should_count_tickets() throws IOException {
        Properties envProps = AggregatingMinMax.loadPropertiesFromConfigFile(TEST_CONFIG_FILE);
        Properties streamProps = AggregatingMinMax.buildStreamsProperties(envProps);

        final MockSchemaRegistryClient srClient = new MockSchemaRegistryClient();

        String inputTopic = envProps.getProperty("input.topic.name");
        String outputTopic = envProps.getProperty("output.topic.name");

        final SpecificAvroSerde<MovieTicketSales> movieTicketSerdes =
                makeMovieTicketSalesSerializer(envProps, srClient);
        final SpecificAvroSerde<YearlyMovieFigures> yearlyFiguresSerdes =
                makeYearlyMovieFiguresSerializer(envProps, srClient);

        final StreamsBuilder builder = new StreamsBuilder();
        Topology topology = AggregatingMinMax.buildTopology(builder, envProps, movieTicketSerdes, yearlyFiguresSerdes);

        try (TopologyTestDriver testDriver = new TopologyTestDriver(topology, streamProps)) {

            TestInputTopic<String, MovieTicketSales> movieTicketSalesTestInputTopic = testDriver.createInputTopic(
                    inputTopic, Serdes.String().serializer(), movieTicketSerdes.serializer());
            TestOutputTopic<Integer, YearlyMovieFigures> movieFiguresTestOutputTopic = testDriver.createOutputTopic(
                    outputTopic, Serdes.Integer().deserializer(), yearlyFiguresSerdes.deserializer());

            movieTicketSalesTestInputTopic.pipeInput(
                    new MovieTicketSales("Avengers: Endgame", 2019, 856980506));

            assertThat(movieFiguresTestOutputTopic.readValue())
                    .isEqualTo(new YearlyMovieFigures(2019, 856980506, 856980506));

//            assertThat(
//                    movieFiguresTestOutputTopic.readValue(),
//                    is(equalTo(new YearlyMovieFigures(2019, 856980506, 856980506))));
//
//            movieTicketSalesTestInputTopic.pipeInput(
//                    new MovieTicketSales("Captain Marvel", 2019, 426829839));
//            assertThat(movieFiguresTestOutputTopic.readValue(),
//                    is(equalTo(new YearlyMovieFigures(2019, 426829839, 856980506))));
//
//            movieTicketSalesTestInputTopic.pipeInput(
//                    new MovieTicketSales("Toy Story 4", 2019, 401486230));
//            assertThat(movieFiguresTestOutputTopic.readValue(),
//                    is(equalTo(new YearlyMovieFigures(2019, 401486230, 856980506))));
//
//            movieTicketSalesTestInputTopic.pipeInput(
//                    new MovieTicketSales("The Lion King", 2019, 385082142));
//            assertThat(movieFiguresTestOutputTopic.readValue(),
//                    is(equalTo(new YearlyMovieFigures(2019, 385082142, 856980506))));
//
//            movieTicketSalesTestInputTopic.pipeInput(
//                    new MovieTicketSales("Black Panther", 2018, 700059566));
//            assertThat(movieFiguresTestOutputTopic.readValue(),
//                    is(equalTo(new YearlyMovieFigures(2018, 700059566,700059566))));
//
//            movieTicketSalesTestInputTopic.pipeInput(
//                    new MovieTicketSales("Avengers: Infinity War", 2018, 678815482));
//            assertThat(movieFiguresTestOutputTopic.readValue(),
//                    is(equalTo(new YearlyMovieFigures(2018,678815482,700059566))));
//
//            movieTicketSalesTestInputTopic.pipeInput(
//                    new MovieTicketSales("Deadpool 2", 2018,324512774));
//            assertThat(movieFiguresTestOutputTopic.readValue(),
//                    is(equalTo(new YearlyMovieFigures(2018,324512774,700059566))));
//
//            movieTicketSalesTestInputTopic.pipeInput(
//                    new MovieTicketSales("Beauty and the Beast", 2017,517218368));
//            assertThat(movieFiguresTestOutputTopic.readValue(),
//                    is(equalTo(new YearlyMovieFigures(2017,517218368,517218368))));
//
//            movieTicketSalesTestInputTopic.pipeInput(
//                    new MovieTicketSales("Wonder Woman", 2017,412563408));
//            assertThat(movieFiguresTestOutputTopic.readValue(),
//                    is(equalTo(new YearlyMovieFigures(2017,412563408,517218368))));
//
//            movieTicketSalesTestInputTopic.pipeInput(
//                    new MovieTicketSales("Star Wars Ep. VIII: The Last Jedi", 2017,517218368));
//            assertThat(movieFiguresTestOutputTopic.readValue(),
//                    is(equalTo(new YearlyMovieFigures(2017,412563408,517218368))));
//
//            assertTrue(movieFiguresTestOutputTopic.isEmpty());
        }
    }
}