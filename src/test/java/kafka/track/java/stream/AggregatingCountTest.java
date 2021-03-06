package kafka.track.java.stream;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import kafka.track.java.avro.TicketSale;
import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

class AggregatingCountTest {

    private final static String TEST_CONFIG_FILE = "aggregate/test.properties";

    private SpecificAvroSerde<TicketSale> makeSerializer(Properties envProps)
            throws IOException, RestClientException {

        final MockSchemaRegistryClient client = new MockSchemaRegistryClient();
        String inputTopic = envProps.getProperty("input.topic.name");
        String outputTopic = envProps.getProperty("output.topic.name");

        final Schema schema = TicketSale.SCHEMA$;
        client.register(inputTopic + "-value", schema);
        client.register(outputTopic + "-value", schema);

        SpecificAvroSerde<TicketSale> serde = new SpecificAvroSerde<>(client);

        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", envProps.getProperty("schema.registry.url"));
        serde.configure(config, false);

        return serde;
    }

    @Test
    void should_count_ticket_sales() throws IOException, RestClientException {
        AggregatingCount aggCount = new AggregatingCount();
        Properties envProps = aggCount.loadEnvProperties(TEST_CONFIG_FILE);
        Properties streamProps = aggCount.buildStreamsProperties(envProps);

        String inputTopic = envProps.getProperty("input.topic.name");
        String outputTopic = envProps.getProperty("output.topic.name");

        final SpecificAvroSerde<TicketSale> ticketSaleSpecificAvroSerde = makeSerializer(envProps);

        Topology topology = aggCount.buildTopology(envProps, ticketSaleSpecificAvroSerde);
        TopologyTestDriver testDriver = new TopologyTestDriver(topology, streamProps);

        Serializer<String> keySerializer = Serdes.String().serializer();
        Deserializer<String> keyDeserializer = Serdes.String().deserializer();

        ConsumerRecordFactory<String, TicketSale>
                inputFactory =
                new ConsumerRecordFactory<>(keySerializer, ticketSaleSpecificAvroSerde.serializer());

        final List<TicketSale>
                input = asList(
                new TicketSale("Die Hard", "2019-07-18T10:00:00Z", 12),
                new TicketSale("Die Hard", "2019-07-18T10:01:00Z", 12),
                new TicketSale("The Godfather", "2019-07-18T10:01:31Z", 12),
                new TicketSale("Die Hard", "2019-07-18T10:01:36Z", 24),
                new TicketSale("The Godfather", "2019-07-18T10:02:00Z", 18),
                new TicketSale("The Big Lebowski", "2019-07-18T11:03:21Z", 12),
                new TicketSale("The Big Lebowski", "2019-07-18T11:03:50Z", 12),
                new TicketSale("The Godfather", "2019-07-18T11:40:00Z", 36),
                new TicketSale("The Godfather", "2019-07-18T11:40:09Z", 18)
        );

        List<Long> expectedOutput = new ArrayList<Long>(Arrays.asList(1L, 2L, 1L, 3L, 2L, 1L, 2L, 3L, 4L));

        for (TicketSale ticketSale : input) {
            testDriver.pipeInput(inputFactory.create(inputTopic, "", ticketSale));
        }

        List<Long> actualOutput = new ArrayList<>();
        while (true) {
            ProducerRecord<String, Long>
                    record =
                    testDriver.readOutput(outputTopic, keyDeserializer, Serdes.Long().deserializer());

            if (record != null) {
                actualOutput.add(record.value());
            } else {
                break;
            }
        }

        System.out.println(actualOutput);
        assertEquals(expectedOutput, actualOutput);

    }

}