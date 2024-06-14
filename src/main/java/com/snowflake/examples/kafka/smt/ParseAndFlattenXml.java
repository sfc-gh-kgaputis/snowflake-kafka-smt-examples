package com.snowflake.examples.kafka.smt;

import com.snowflake.examples.kafka.utils.XmlParsingException;
import com.snowflake.examples.kafka.utils.XmlUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ParseAndFlattenXml implements Transformation<SinkRecord> {
    StringConverter d = null;
    private static final Logger logger = LoggerFactory.getLogger(ParseAndFlattenXml.class);

    public String getLoggingIdentifier(SinkRecord record) {
        // Avoid record values in logging to prevent data leaks
        return "[topic: " + record.topic() + ", partition: " + record.kafkaPartition() + ", offset: " + record.kafkaOffset() + "]";
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // Configuration options can be set here
    }

    @Override
    public SinkRecord apply(SinkRecord record) {
        if (record.value() == null) {
            return record;
        }
        Object rawValue = record.value();
        String xmlData;
        if (rawValue instanceof String) {
            xmlData = (String) rawValue;
        } else if (rawValue instanceof byte[]) {
            try {
                //TODO make encoding configurable at the SMT level
                xmlData = new String((byte[]) rawValue, StandardCharsets.UTF_8);
            } catch (Exception e) {
                logger.error("Unable to convert byte array to string for message: " + getLoggingIdentifier(record), e);
                throw new DataException("Unable to convert byte array to string", e);
            }
        } else {
            logger.error("Unsupported data type for XML flattening for message: " + getLoggingIdentifier(record));
            throw new DataException("Unsupported data type for XML flattening");
        }
        Map<String, String> parsedXmlData;
        try {
            parsedXmlData = XmlUtils.parseAndValidateXML(xmlData);
        } catch (XmlParsingException e) {
            logger.error("Error parsing XML data for message: " + getLoggingIdentifier(record));
            throw new DataException("Error parsing XML data");
        }

        // Return a new SinkRecord with the parsed and flattened data from XML
        // NOTE: Make sure that valueSchema is null, so record will be considered as schemaless JSON
        return new SinkRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), null,
                parsedXmlData, // The parsed XML data
                record.kafkaOffset(), record.timestamp(), record.timestampType());
    }


    @Override
    public void close() {
        // Clean up any resources if required
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef(); // Define configuration here if needed
    }
}
