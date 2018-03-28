package demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class KafkaSender {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    String kafkaTopic = "test";

    public void send(String message) {
//        kafkaTemplate.send(kafkaTopic, message);
        kafkaTemplate.setDefaultTopic(kafkaTopic);
        kafkaTemplate.sendDefault(UUID.randomUUID().toString(), message);
    }
}
