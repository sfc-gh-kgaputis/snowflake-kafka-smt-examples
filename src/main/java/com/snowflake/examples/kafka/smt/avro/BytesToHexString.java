package com.snowflake.examples.kafka.smt.avro;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Kafka Connect SMT that converts byte array fields to hex-encoded strings.
 * 
 * <p>This transformation converts BYTES schema fields to STRING schema and their values to hex-encoded strings.
 * This maintains schema/value contract integrity required by Kafka Connect.
 * 
 * <p>Example configuration:
 * <pre>
 * transforms=bytesToHex
 * transforms.bytesToHex.type=com.snowflake.examples.kafka.smt.avro.BytesToHexString$Value
 * transforms.bytesToHex.uppercase=true
 * </pre>
 * 
 * <p><b>Note:</b> Snowflake's TRY_TO_BINARY(hex_string, 'HEX') function does not accept a '0x' prefix.
 * Leave the prefix setting empty (default) when converting back to binary in Snowflake.
 * 
 * @param <R> the record type (SourceRecord or SinkRecord)
 */
public abstract class BytesToHexString<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(BytesToHexString.class);

    public static final String OVERVIEW_DOC = 
            "Convert all BYTES fields to hex-encoded STRING fields in deeply nested schemas. "
            + "The transformation recursively processes Structs, Arrays, and Maps. "
            + "<p/>Use the concrete transformation type designed for the record key (<code>" 
            + Key.class.getName() + "</code>) or value (<code>" + Value.class.getName() + "</code>).";

    // Configuration keys
    public static final String PREFIX_CONFIG = "prefix";
    public static final String UPPERCASE_CONFIG = "uppercase";
    public static final String CACHE_SIZE_CONFIG = "cache.size";

    // Configuration documentation
    private static final String PREFIX_DOC = "Optional prefix to add to hex strings (e.g., '0x'). Note: Snowflake's TRY_TO_BINARY() does not accept a prefix.";
    private static final String UPPERCASE_DOC = "Use uppercase letters for hex encoding (A-F vs a-f)";
    private static final String CACHE_SIZE_DOC = "Size of the schema cache";

    // Default values
    private static final String DEFAULT_PREFIX = "";
    private static final boolean DEFAULT_UPPERCASE = false;
    private static final int DEFAULT_CACHE_SIZE = 16;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
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
        String prefix = config.getString(PREFIX_CONFIG);
        boolean uppercase = config.getBoolean(UPPERCASE_CONFIG);
        int cacheSize = config.getInt(CACHE_SIZE_CONFIG);

        // Initialize components
        HexConverter hexConverter = new HexConverter(prefix, uppercase);
        this.schemaTransformer = new SchemaTransformer();
        this.valueTransformer = new ValueTransformer(hexConverter);
        this.schemaCache = new SynchronizedCache<>(new LRUCache<>(cacheSize));

        log.info("Configured BytesToHexString with prefix='{}', uppercase={}", 
                prefix, uppercase);
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
    public static final class Key<R extends ConnectRecord<R>> extends BytesToHexString<R> {
        
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
    public static final class Value<R extends ConnectRecord<R>> extends BytesToHexString<R> {
        
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

