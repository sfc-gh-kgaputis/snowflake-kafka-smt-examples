package com.snowflake.examples.kafka.smt;

import com.snowflake.examples.kafka.utils.XmlParsingException;
import com.snowflake.examples.kafka.utils.XmlUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Map;

public class ParseAndFlattenXml implements Transformation<SinkRecord> {
    private Charset xmlCharset = null;
    private static final Logger logger = LoggerFactory.getLogger(ParseAndFlattenXml.class);

    public String getLoggingIdentifier(SinkRecord record) {
        // Avoid record values in logging to prevent data leaks
        return "[topic: " + record.topic() + ", partition: " + record.kafkaPartition() + ", offset: " + record.kafkaOffset() + "]";
    }

    @Override
    public void configure(Map<String, ?> configs) {
        String charsetName = (String) configs.get("encoding");
        logger.info("Received config encoding: " + charsetName);
        if (charsetName == null || charsetName.isEmpty()) {
            throw new ConfigException("Missing or invalid config: encoding");
        }
        try {
            xmlCharset = Charset.forName(charsetName);
        } catch (Exception e) {
            throw new ConfigException("Unable to load character set: " + charsetName, e);
        }
    }

    @Override
    public SinkRecord apply(SinkRecord record) {
        //TODO validate that bytes are received when xmlCharset is not null
        if (record.value() == null) {
            return record;
        }
        Object rawValue = record.value();
        String xmlData;
        if (rawValue instanceof String) {
            xmlData = (String) rawValue;
        } else if (rawValue instanceof byte[]) {
            try {
                xmlData = new String((byte[]) rawValue, xmlCharset);
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
        return new ConfigDef()
                .define("encoding", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Expected encoding of XML data, as a Java charset name");
    }

}
