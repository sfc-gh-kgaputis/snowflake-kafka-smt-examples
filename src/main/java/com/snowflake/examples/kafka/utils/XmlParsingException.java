package com.snowflake.examples.kafka.utils;


public class XmlParsingException extends Exception {
    public XmlParsingException(String message) {
        super(message);
    }

    public XmlParsingException(String message, Throwable cause) {
        super(message, cause);
    }
}
