//package com.dip.kafka_consumer.service;
//
//import com.dip.kafka_consumer.dto.SignedMessage;
//import com.fasterxml.jackson.core.type.TypeReference;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.kafka.clients.consumer.*;
//import org.apache.kafka.common.TopicPartition;
//import org.springframework.stereotype.Service;
//
//
//import java.io.BufferedReader;
//import java.io.FileReader;
//import java.io.IOException;
//import java.security.*;
//import java.security.spec.InvalidKeySpecException;
//import java.security.spec.RSAPublicKeySpec;
//import java.time.Duration;
//import java.time.LocalDateTime;
//import java.util.*;
//import java.util.concurrent.atomic.AtomicLong;
//
//@Service
//public class ConsumerCustomInterceptor implements ConsumerInterceptor<String,String> {
//
//
//    @Override
//    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
//        AtomicLong totalDecryptionTime = new AtomicLong();
//
//        records.forEach(record -> {
//            List<SignedMessage> signedMessageList = null;
//            ObjectMapper objectMapper = new ObjectMapper();
//            System.out.println("---------------------Decryption Started _________________________");
//            try {
//                signedMessageList = objectMapper.readValue(record.value().toString(), new TypeReference<ArrayList<SignedMessage>>() {
//                });
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            for(SignedMessage signedMessage: signedMessageList) {
//                try {
//                    LocalDateTime startSendingTime = LocalDateTime.now();
//                    String publicKeyPath = System.getProperty("user.home").concat("\\public_key.txt");
//                    //        String publicKeyPath = System.getProperty("user.home").concat("\\Invalid_public_key.txt");
//                    PublicKey publicKey = readPublicKeyFromFile(publicKeyPath);
//                    byte[] employeeAsByte = objectMapper.writeValueAsBytes(signedMessage.getEmployeeId());
//                    Signature signature = Signature.getInstance("SHA256withRSA");
//                    signature.initVerify(publicKey);
//                    signature.update(employeeAsByte);
//                    boolean signatureValid = signature.verify(signedMessage.getSignature());
//                    LocalDateTime endProcessingTime = LocalDateTime.now();
//                    System.out.println("validation : :" + signatureValid);
//                    if (!signatureValid) {
//                        return new ConsumerRecords<>(new HashMap<>());
//                    }
//                    long durationMillis = Duration.between(startSendingTime, endProcessingTime).toMillis();
//                    totalDecryptionTime.addAndGet(durationMillis);
//                } catch (Exception ex) {
//                    System.err.println("Error processing message with key: " + ex.getMessage());
//                    throw new RuntimeException("Exception");
//                }
//            };
//
//            System.out.println("Time taken to decrypt and validate the message " + record.key() + " :  " + totalDecryptionTime + " milliseconds");
//            System.out.println("---------------------Decryption Done for " + record.key() + " record------------------------------");
//
//        });
//
//        return records;
//    }
//
//
////        if (recordCount.get() > 0) {
////            double averageDecryptionTime = (double) totalDecryptionTime.get() / recordCount.get();
////            System.out.println("************* Average Decryption Time for " + recordCount + " records: " + averageDecryptionTime + " ms *********");
////        } else {
////            System.out.println("No records were processed.");
////        }
////        System.out.println("\n");
////        return records;
////    }
//
//   // }
//
//    private PublicKey readPublicKeyFromFile(String publicKeyFilePath) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
//        PublicKey publicKey =null;
//        try {
//            BufferedReader br = new BufferedReader(new FileReader(publicKeyFilePath));
//            String line;
//            String modulus = null;
//            String publicExponent = null;
//
//            while ((line = br.readLine()) != null) {
//                if (line.contains("modulus: ")) {
//                    modulus = line.split("modulus: ")[1].trim();
//                } else if (line.contains("public exponent: ")) {
//                    publicExponent = line.split("public exponent: ")[1].trim();
//                }
//            }
//
//            br.close();
//
//            // Parse modulus and public exponent as BigIntegers
//            java.math.BigInteger modulusBigInt = new java.math.BigInteger(modulus);
//            java.math.BigInteger publicExponentBigInt = new java.math.BigInteger(publicExponent);
//
//            RSAPublicKeySpec rsaPublicKeySpec = new RSAPublicKeySpec(modulusBigInt, publicExponentBigInt);
//
//            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
//            publicKey = keyFactory.generatePublic(rsaPublicKeySpec);
//            // You now have a RSAPublicKey object in the publicKey variable
//        } catch (Exception e) {
//            throw  new RuntimeException("Exception in readPublicKeyFromFile : "+e.getMessage());
//        }
//        return publicKey;
//    }
//
//
//
//    @Override
//    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {
//
//    }
//
//    @Override
//    public void close() {
//
//    }
//
//    @Override
//    public void configure(Map<String, ?> map) {
//
//    }
//}
