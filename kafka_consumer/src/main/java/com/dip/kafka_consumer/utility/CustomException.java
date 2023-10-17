package com.dip.kafka_consumer.utility;

public class CustomException extends RuntimeException{

    private String message;

    public CustomException(String message) {
        throw new  RuntimeException(message);
    }
}
