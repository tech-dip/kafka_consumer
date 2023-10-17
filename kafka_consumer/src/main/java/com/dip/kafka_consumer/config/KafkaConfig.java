package com.dip.kafka_consumer.config;

import com.dip.kafka_consumer.dto.SignedMessage;
import com.dip.kafka_consumer.utility.CustomException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPublicKeySpec;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Configuration
public class KafkaConfig {

    @Value("${kafka.dummyPublicKey}")
    private byte[] dummyPublicKey;

    @Bean
    ConsumerFactory<String,Object> consumerFactory(MeterRegistry meterRegistry) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put("compression.topic", "signed_message");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "group_id");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
       // config.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ConsumerCustomInterceptor.class.getName());
        DefaultKafkaConsumerFactory<String,Object> consumerFactory = new DefaultKafkaConsumerFactory<>(config);
        consumerFactory.addListener(new MicrometerConsumerListener<>(meterRegistry));
        return consumerFactory;
    }
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(MeterRegistry meterRegistry) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory(meterRegistry));

        factory.setRecordFilterStrategy(consumerRecord -> {
            System.out.println("In Consumer filter"+ LocalDateTime.now());
            ObjectMapper objectMapper = new ObjectMapper();
                List<SignedMessage> signedMessageList = null;
                try {
                    signedMessageList = objectMapper.readValue(consumerRecord.value().toString(), new TypeReference<ArrayList<SignedMessage>>() {
                    });
                } catch (Exception e) {
                    System.err.println("Error processing : " + e.getMessage());
                    throw  new CustomException("Exception" +e.getMessage());
                }
            AtomicLong totalDecryptionTime = new AtomicLong(0);
            for(SignedMessage signedMessage: signedMessageList) {
                    try {
                        System.out.println("---------------------Decryption Started  _________________________");
                        LocalDateTime startSendingTime = LocalDateTime.now();
                        System.out.println("*** startSendingTime:"+startSendingTime+"*****");
                        String publicKeyPath = System.getProperty("user.home").concat("\\public_key.txt");
                        PublicKey publicKey = readPublicKeyFromFile(publicKeyPath);
                        byte[] employeeAsByte = objectMapper.writeValueAsBytes(signedMessage.getEmployeeId());
                        Signature signature = Signature.getInstance("SHA256withRSA");
                        signature.initVerify(publicKey);
                        signature.update(employeeAsByte);
                        boolean signatureValid = signature.verify(signedMessage.getSignature());
                        LocalDateTime endProcessingTime = LocalDateTime.now();
                        System.out.println("*** endProcessingTime:"+endProcessingTime+"*****");

                        System.out.println("validation is : :" + signatureValid);
                        if (!signatureValid) {
                            return true;
                        }
                        long durationMillis = Duration.between(startSendingTime, endProcessingTime).toMillis();

                        totalDecryptionTime.getAndAdd(durationMillis);
                        System.out.println("---------------------Decryption Completed _________________________");

                    } catch (Exception ex) {
                        System.err.println("Error processing message with key: " + ex.getMessage());
                        throw new RuntimeException("Exception");
                    }
                }
            double averageTime = (totalDecryptionTime.get()) /  signedMessageList.size();
            System.out.println("------Average time taken------" +averageTime +" milliSecond And  Number of record is : "+signedMessageList.size()+"_________________________");
            return false;

        });
        return factory;
    }


        private PublicKey readPublicKeyFromFile(String publicKeyFilePath) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException
        {
            PublicKey publicKey =null;
            try {
                BufferedReader br = new BufferedReader(new FileReader(publicKeyFilePath));
                String line;
                String modulus = null;
                String publicExponent = null;

                while ((line = br.readLine()) != null) {
                    if (line.contains("modulus: ")) {
                        modulus = line.split("modulus: ")[1].trim();
                    } else if (line.contains("public exponent: ")) {
                        publicExponent = line.split("public exponent: ")[1].trim();
                    }
                }

                br.close();

                // Parse modulus and public exponent as BigIntegers
                java.math.BigInteger modulusBigInt = new java.math.BigInteger(modulus);
                java.math.BigInteger publicExponentBigInt = new java.math.BigInteger(publicExponent);

                RSAPublicKeySpec rsaPublicKeySpec = new RSAPublicKeySpec(modulusBigInt, publicExponentBigInt);

                KeyFactory keyFactory = KeyFactory.getInstance("RSA");
                publicKey = keyFactory.generatePublic(rsaPublicKeySpec);
                // You now have a RSAPublicKey object in the publicKey variable
            } catch (Exception e) {
                throw  new RuntimeException("Exception in readPublicKeyFromFile : "+e.getMessage());
            }
            return publicKey;
        }

}