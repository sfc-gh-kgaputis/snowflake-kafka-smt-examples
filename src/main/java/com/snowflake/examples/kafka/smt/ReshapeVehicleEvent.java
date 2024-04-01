package com.snowflake.examples.kafka.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReshapeVehicleEvent implements Transformation<SinkRecord> {

    private static final Logger log = LoggerFactory.getLogger(ReshapeVehicleEvent.class);
    public static final String FIELD_TIMESTAMP = "timestamp";
    public static final String FIELD_VIN = "vin";
    public static final String FIELD_TYPE = "type";
    public static final String FIELD_PAYLOAD = "payload";

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
        // It's probably ok to reshape a record with a schema, but fail for now
        if (record.valueSchema() != null) {
            throw new DataException("Not reshaping record due to unexpected schema: " + getLoggingIdentifier(record));
        }
        // Verify that the record value is a Map (expected for schemaless JSON)
        if (!(record.value() instanceof Map)) {
            throw new DataException("Not reshaping record because value is not a Map: " + getLoggingIdentifier(record));
        }

        @SuppressWarnings("unchecked") Map<String, Object> originalMap = (Map<String, Object>) record.value();
        Map<String, Object> newMap = new HashMap<>();
        Map<String, Object> payloadData = new HashMap<>();

        // List to keep track of missing fields
        List<String> missingFields = new ArrayList<>();
        // Check for the presence of required fields
        // Not all fields need to be required
        if (!originalMap.containsKey(FIELD_TIMESTAMP)) {
            missingFields.add(FIELD_TIMESTAMP);
        }
        if (!originalMap.containsKey(FIELD_VIN)) {
            missingFields.add(FIELD_VIN);
        }
        if (!originalMap.containsKey(FIELD_TYPE)) {
            missingFields.add(FIELD_TYPE);
        }
        // If there are missing fields, throw an exception
        if (!missingFields.isEmpty()) {
            throw new DataException("Record is missing required fields: " + String.join(", ", missingFields) + ", record: " + getLoggingIdentifier(record));
        }

        for (Map.Entry<String, Object> entry : originalMap.entrySet()) {
            switch (entry.getKey()) {
                case FIELD_TIMESTAMP:
                case FIELD_VIN:
                case FIELD_TYPE:
                    newMap.put(entry.getKey(), entry.getValue());
                    break;
                default:
                    payloadData.put(entry.getKey(), entry.getValue());
                    break;
            }
        }

        newMap.put(FIELD_PAYLOAD, payloadData);

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
        return new ConfigDef(); // Define configuration here if needed
    }
}
