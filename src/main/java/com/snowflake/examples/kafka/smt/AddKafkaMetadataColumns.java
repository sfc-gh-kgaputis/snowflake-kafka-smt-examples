package com.snowflake.examples.kafka.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class AddKafkaMetadataColumns implements Transformation<SinkRecord> {

    private static final Logger log = LoggerFactory.getLogger(AddKafkaMetadataColumns.class);

    private String columnNameKafkaTopic;
    private String columnNameKafkaPartition;
    private String columnNameKafkaOffset;

    public String getLoggingIdentifier(SinkRecord record) {
        // Avoid record values in logging to prevent data leaks
        return "[topic: " + record.topic() + ", partition: " + record.kafkaPartition() + ", offset: " + record.kafkaOffset() + "]";
    }

    @Override
    public void configure(Map<String, ?> configs) {
        columnNameKafkaTopic = (String) configs.get("columnNameKafkaTopic");
        log.info("Received config columnNameKafkaTopic: " + columnNameKafkaTopic);
        if (columnNameKafkaTopic == null || columnNameKafkaTopic.isEmpty()) {
            throw new ConfigException("Missing or invalid config: columnNameKafkaTopic");
        }
        columnNameKafkaPartition = (String) configs.get("columnNameKafkaPartition");
        log.info("Received config columnNameKafkaPartition: " + columnNameKafkaPartition);
        if (columnNameKafkaPartition == null || columnNameKafkaPartition.isEmpty()) {
            throw new ConfigException("Missing or invalid config: columnNameKafkaPartition");
        }
        columnNameKafkaOffset = (String) configs.get("columnNameKafkaOffset");
        log.info("Received config columnNameKafkaOffset: " + columnNameKafkaOffset);
        if (columnNameKafkaOffset == null || columnNameKafkaOffset.isEmpty()) {
            throw new ConfigException("Missing or invalid config: columnNameKafkaOffset");
        }
    }

    @Override
    public SinkRecord apply(SinkRecord record) {
        // It's probably ok to modify a record with a schema, but fail for now
        if (record.valueSchema() != null) {
            throw new DataException("Not modifying record due to unexpected schema: " + getLoggingIdentifier(record));
        }
        // Verify that the record value is a Map (expected for schemaless JSON)
        if (!(record.value() instanceof Map)) {
            throw new DataException("Not modifying record because value is not a Map: " + getLoggingIdentifier(record));
        }

        @SuppressWarnings("unchecked") Map<String, Object> originalMap = (Map<String, Object>) record.value();
        Map<String, Object> newMap = new HashMap<>(originalMap);

        // Add Kafka metadata fields to the map
        newMap.put(columnNameKafkaTopic, record.topic());
        newMap.put(columnNameKafkaPartition, record.kafkaPartition());
        newMap.put(columnNameKafkaOffset, record.kafkaOffset());

        // Return a new SinkRecord with the reshaped data
        return new SinkRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), null, // No value schema since the data is schema-less
                newMap, // The reshaped value
                record.kafkaOffset(), record.timestamp(), record.timestampType());

    }

    @Override
    public void close() {
        // Clean up any resources if required
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define("columnNameKafkaTopic", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Target column name for Kafka topic metadata")
                .define("columnNameKafkaPartition", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Target column name for Kafka partition metadata")
                .define("columnNameKafkaOffset", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Target column name for Kafka offset metadata");
    }
}
