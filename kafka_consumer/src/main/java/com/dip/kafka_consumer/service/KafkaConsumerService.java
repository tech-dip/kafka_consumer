package com.dip.kafka_consumer.service;

import com.dip.kafka_consumer.dto.SignedMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;


@Service
public class KafkaConsumerService {

    private final Counter kafkaMessageCounter;

    public KafkaConsumerService(MeterRegistry meterRegistry) {
        this.kafkaMessageCounter = Counter.builder("kafka_messages")
                .description("Total Kafka messages received")
                .register(meterRegistry);
    }

    @KafkaListener(topics = "signed_message")
    public void consumeDocument(ConsumerRecord <String, Object> consumerRecords) {
        LocalDateTime startSendingTime = LocalDateTime.now();
        ObjectMapper objectMapper = new ObjectMapper();
             System.out.println("In Consumer service.. "+ LocalDateTime.now());
                try {
                    List<SignedMessage> signedMessageList  = objectMapper.readValue(consumerRecords.value().toString(), new TypeReference<ArrayList<SignedMessage>>() {});
                    signedMessageList.stream().forEach(message-> {
                        int employee = message.getEmployeeId(); // Use the appropriate character encoding
                        String firstName = message.getEmp_firstname();
                        String lastName = message.getEmp_lastname();
                        System.out.println("Message received at Consumer: Employee ID - " + employee +
                                ", First Name - " + firstName + ", Last Name - " + lastName);
                        System.out.println("---------------------------------------");
                    });
                kafkaMessageCounter.increment();
                LocalDateTime endProcessingTime = LocalDateTime.now();
                System.out.println();
                System.out.println("message sent and processed at " + endProcessingTime);
                long durationMillis = Duration.between(startSendingTime, endProcessingTime).toMillis();
                System.out.println("Time taken to receive and process the message: " + durationMillis + " milliseconds");
                } catch (Exception ex) {
                throw  new RuntimeException("Exception occur"+ex.getMessage());
            }
            }
        }