package com.snowflake.examples.kafka.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class AddSchemaIdHeader<S> implements Transformation<SinkRecord> {

    public static final String SCHEMA_ID_HEADER = "schema_id";

    @Override
    public void configure(Map<String, ?> configs) {
        // Configuration options can be set here
    }

    @Override
    public SinkRecord apply(SinkRecord record) {
        Headers headers = new ConnectHeaders(record.headers());
        headers.addString(SCHEMA_ID_HEADER, record.valueSchema().version().toString());

        return new SinkRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value(),
                record.kafkaOffset(),
                record.timestamp(),
                record.timestampType(),
                headers
        );
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
