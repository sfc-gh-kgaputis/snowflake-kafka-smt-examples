package com.snowflake.examples.kafka.smt.avro;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Kafka Connect SMT that converts byte array fields to encoded strings (hex or base64).
 * 
 * <p>This transformation converts BYTES schema fields to STRING schema and their values to encoded strings.
 * This maintains schema/value contract integrity required by Kafka Connect.
 * 
 * <p><b>Recommended:</b> Use BASE64 encoding for better efficiency and compatibility with Snowflake's 
 * High-Performance Streaming Architecture.
 * 
 * <p><b>Decimal Handling:</b> Automatically handles Kafka Connect Decimal logical types (BigDecimal values).
 * When convertDecimalsToString=true (default), Decimals are converted to human-readable strings.
 * When false, Decimal fields are skipped. This is useful for Debezium CDC streams.
 * 
 * <p>Example configuration (base64 - recommended):
 * <pre>
 * transforms=bytesToString
 * transforms.bytesToString.type=com.snowflake.examples.kafka.smt.avro.BytesToEncodedString$Value
 * transforms.bytesToString.encoding=base64
 * </pre>
 * 
 * <p>Example configuration (hex):
 * <pre>
 * transforms=bytesToString
 * transforms.bytesToString.type=com.snowflake.examples.kafka.smt.avro.BytesToEncodedString$Value
 * transforms.bytesToString.encoding=hex
 * transforms.bytesToString.uppercase=true
 * </pre>
 * 
 * <p>Example configuration (skip decimals):
 * <pre>
 * transforms=bytesToString
 * transforms.bytesToString.type=com.snowflake.examples.kafka.smt.avro.BytesToEncodedString$Value
 * transforms.bytesToString.encoding=base64
 * transforms.bytesToString.convertDecimalsToString=false
 * </pre>
 * 
 * @param <R> the record type (SourceRecord or SinkRecord)
 */
public abstract class BytesToEncodedString<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(BytesToEncodedString.class);

    public static final String OVERVIEW_DOC = 
            "Convert all BYTES fields to encoded STRING fields in deeply nested schemas. "
            + "Supports hex and base64 encoding. Base64 is recommended for efficiency. "
            + "The transformation recursively processes Structs, Arrays, and Maps. "
            + "<p/>Use the concrete transformation type designed for the record key (<code>" 
            + Key.class.getName() + "</code>) or value (<code>" + Value.class.getName() + "</code>).";

    // Configuration keys
    public static final String ENCODING_CONFIG = "encoding";
    public static final String PREFIX_CONFIG = "prefix";
    public static final String UPPERCASE_CONFIG = "uppercase";
    public static final String CONVERT_DECIMALS_CONFIG = "convertDecimalsToString";
    public static final String CACHE_SIZE_CONFIG = "cache.size";

    // Configuration documentation
    private static final String ENCODING_DOC = "Encoding format: 'hex' or 'base64'. Base64 is recommended for efficiency.";
    private static final String PREFIX_DOC = "Optional prefix to add to encoded strings (e.g., '0x' for hex). Note: Snowflake functions do not accept prefixes.";
    private static final String UPPERCASE_DOC = "Use uppercase letters for hex encoding (A-F vs a-f). Only applies to hex encoding.";
    private static final String CONVERT_DECIMALS_DOC = "Convert Decimal logical types (BigDecimal) to human-readable strings. When false, Decimal fields are skipped.";
    private static final String CACHE_SIZE_DOC = "Size of the schema cache";

    // Default values
    private static final String DEFAULT_ENCODING = "base64";
    private static final String DEFAULT_PREFIX = "";
    private static final boolean DEFAULT_UPPERCASE = false;
    private static final boolean DEFAULT_CONVERT_DECIMALS = true;
    private static final int DEFAULT_CACHE_SIZE = 16;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ENCODING_CONFIG,
                    ConfigDef.Type.STRING,
                    DEFAULT_ENCODING,
                    ConfigDef.Importance.HIGH,
                    ENCODING_DOC)
            .define(CONVERT_DECIMALS_CONFIG,
                    ConfigDef.Type.BOOLEAN,
                    DEFAULT_CONVERT_DECIMALS,
                    ConfigDef.Importance.MEDIUM,
                    CONVERT_DECIMALS_DOC)
            .define(PREFIX_CONFIG,
                    ConfigDef.Type.STRING,
                    DEFAULT_PREFIX,
                    ConfigDef.Importance.LOW,
                    PREFIX_DOC)
            .define(UPPERCASE_CONFIG,
                    ConfigDef.Type.BOOLEAN,
                    DEFAULT_UPPERCASE,
                    ConfigDef.Importance.LOW,
                    UPPERCASE_DOC)
            .define(CACHE_SIZE_CONFIG,
                    ConfigDef.Type.INT,
                    DEFAULT_CACHE_SIZE,
                    ConfigDef.Range.atLeast(1),
                    ConfigDef.Importance.LOW,
                    CACHE_SIZE_DOC);

    // Components - each handles one specific responsibility
    private SchemaTransformer schemaTransformer;
    private ValueTransformer valueTransformer;
    private Cache<Schema, Schema> schemaCache;

    @Override
    public void configure(Map<String, ?> props) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

        // Read configuration
        String encodingStr = config.getString(ENCODING_CONFIG);
        String prefix = config.getString(PREFIX_CONFIG);
        boolean uppercase = config.getBoolean(UPPERCASE_CONFIG);
        boolean convertDecimals = config.getBoolean(CONVERT_DECIMALS_CONFIG);
        int cacheSize = config.getInt(CACHE_SIZE_CONFIG);

        // Parse encoding
        ByteEncoder.Encoding encoding;
        try {
            encoding = ByteEncoder.Encoding.valueOf(encodingStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new ConfigException("Invalid encoding: " + encodingStr + ". Must be 'hex' or 'base64'");
        }

        // Initialize components
        ByteEncoder byteEncoder = new ByteEncoder(encoding, prefix, uppercase);
        this.schemaTransformer = new SchemaTransformer(convertDecimals);
        this.valueTransformer = new ValueTransformer(byteEncoder, convertDecimals);
        this.schemaCache = new SynchronizedCache<>(new LRUCache<>(cacheSize));

        log.info("Configured BytesToEncodedString with encoding='{}', prefix='{}', uppercase={}, convertDecimalsToString={}", 
                encodingStr, prefix, uppercase, convertDecimals);
    }

    @Override
    public R apply(R record) {
        Object value = getRecordValue(record);
        Schema schema = getRecordSchema(record);

        // Early return if nothing to transform
        if (value == null || schema == null) {
            return record;
        }

        // Transform both schema and value (BYTES -> STRING)
        Schema targetSchema = getTransformedSchema(schema);
        
        // If schema didn't change, no BYTES fields exist
        if (targetSchema == schema) {
            return record;
        }
        
        Object transformedValue;
        if (schema.type() == Schema.Type.STRUCT) {
            // For top-level structs, pass both original and transformed schemas
            transformedValue = valueTransformer.transformStruct(
                (org.apache.kafka.connect.data.Struct) value,
                schema,
                targetSchema
            );
        } else {
            // For other types, use the regular transform method
            transformedValue = valueTransformer.transform(value, schema);
        }

        // Create new record with transformed schema and value
        return createRecord(record, targetSchema, transformedValue);
    }

    /**
     * Get the transformed schema from cache, or transform and cache it.
     */
    private Schema getTransformedSchema(Schema originalSchema) {
        Schema cached = schemaCache.get(originalSchema);
        if (cached != null) {
            return cached;
        }

        Schema transformed = schemaTransformer.transform(originalSchema);
        schemaCache.put(originalSchema, transformed);
        return transformed;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        if (schemaCache != null) {
            schemaCache = null;
        }
    }

    // Abstract methods - implemented by Key and Value subclasses

    /**
     * Get the schema from the record (key or value schema).
     */
    protected abstract Schema getRecordSchema(R record);

    /**
     * Get the value from the record (key or value).
     */
    protected abstract Object getRecordValue(R record);

    /**
     * Create a new record with the transformed schema and value.
     */
    protected abstract R createRecord(R record, Schema transformedSchema, Object transformedValue);

    /**
     * Transformation for record keys.
     */
    public static final class Key<R extends ConnectRecord<R>> extends BytesToEncodedString<R> {
        
        @Override
        protected Schema getRecordSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object getRecordValue(R record) {
            return record.key();
        }

        @Override
        protected R createRecord(R record, Schema transformedSchema, Object transformedValue) {
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    transformedSchema,
                    transformedValue,
                    record.valueSchema(),
                    record.value(),
                    record.timestamp()
            );
        }
    }

    /**
     * Transformation for record values.
     */
    public static final class Value<R extends ConnectRecord<R>> extends BytesToEncodedString<R> {
        
        @Override
        protected Schema getRecordSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object getRecordValue(R record) {
            return record.value();
        }

        @Override
        protected R createRecord(R record, Schema transformedSchema, Object transformedValue) {
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    transformedSchema,
                    transformedValue,
                    record.timestamp()
            );
        }
    }
}

