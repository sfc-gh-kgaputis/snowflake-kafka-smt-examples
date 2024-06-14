package com.snowflake.examples.kafka.converter;

import org.apache.kafka.connect.storage.StringConverter;

/**
 * * This is a simple wrapper class of StringConverter.
 * * Because it's named differently, the Snowflake Connector doesn't throw an error that this value converter isn't supported with schematization.
 * * NOTE: you should only use this converter if you plan to transform the ByteArray into a schematized record via SMT.
 */
public class StringValueConverter extends StringConverter {
}
