package com.snowflake.examples.kafka.converter;

import org.apache.kafka.connect.converters.ByteArrayConverter;

/**
 * This is a simple wrapper class of ByteArrayConverter.
 * Because it's named differently, the Snowflake Connector doesn't throw an error that this value converter isn't supported with schematization.
 * NOTE: you should only use this converter if you plan to transform the ByteArray into a schematized record via SMT.
 */
public class ByteArrayValueConverter extends ByteArrayConverter {
}
