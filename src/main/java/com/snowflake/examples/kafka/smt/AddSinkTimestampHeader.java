package com.snowflake.examples.kafka.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class AddSinkTimestampHeader<S> implements Transformation<SinkRecord> {

    public static final String TIMESTAMP_HEADER = "sink_timestamp_millis";

    @Override
    public void configure(Map<String, ?> configs) {
        // Configuration options can be set here
    }

    @Override
    public SinkRecord apply(SinkRecord record) {
        Headers headers = new ConnectHeaders(record.headers());
        headers.addString(TIMESTAMP_HEADER, String.valueOf(System.currentTimeMillis()));

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
