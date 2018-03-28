package demo;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

//@SpringBootApplication
//public class DemoApplication {
//
//	public static void main(String[] args) {
//		SpringApplication.run(DemoApplication.class, args);
//	}
//}


public class DemoApplication {

    public static void main(String[] args) throws Exception {


        StreamsConfig streamsConfig = new StreamsConfig(getProperties());
        Serde<String> stringSerde = Serdes.String();

        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        kStreamBuilder.stream(stringSerde, stringSerde, "text")
                .flatMapValues(text -> Arrays.asList(text.split(" ")))
                .map((key, word) -> new KeyValue<>(word, word))
//                .countByKey(stringSerde, "Counts")
//                .toStream()
                .map((word, count) -> new KeyValue<>(word, word + ":" + count))
                .to(stringSerde, stringSerde, "result");

        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, streamsConfig);
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "Example-Counter");
        props.put("group.id", "test-group");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-counter");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
}

