package demo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.web.bind.annotation.*;

import java.util.Date;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;


import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

@RestController
@RequestMapping(value = "/")
public class ApacheKafkaWebController {

    @Autowired
    private demo.KafkaSender kafkaSender;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ProducerFactory producerFactory;

    @Autowired
    private ObjectMapper objectMapper;

    @GetMapping(value = "/producer")
    public String producer(@RequestParam("message") String message) {
        kafkaSender.send(message);
        return "Message sent to the Kafka Topic java_in_use_topic Successfully";
    }

    @PostMapping(value = "/order")
    public void createOrder() throws JsonProcessingException {
        CreateOrder.User user = new CreateOrder.User();
        user.setId("3123sadee312das1");
        user.setFirstName("Maksim");
        user.setLastName("Lukaretskiy");

        CreateOrder createOrderEvent = new CreateOrder();
        createOrderEvent.setDate(new Date());
        createOrderEvent.setUser(user);

        String json = objectMapper.writeValueAsString(createOrderEvent);

        kafkaTemplate.send("test", json);
    }

    @KafkaListener(id ="test", topics = "test")
    public void listen(ConsumerRecord<?, ?> cr) throws Exception {
        System.out.println(cr.toString());
    }

}
