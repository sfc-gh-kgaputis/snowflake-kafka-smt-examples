package com.snowflake.examples.kafka.smt.avro;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Example usage and test cases for BytesToHexString transformation.
 */
public class BytesToHexStringTest {

    /**
     * Example 1: Simple struct with a single BYTES field
     */
    @Test
    public void exampleSimpleBytesField() {
        // Create schema with a BYTES field
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("data", Schema.BYTES_SCHEMA)
                .build();

        // Create value
        Struct value = new Struct(schema)
                .put("id", 123)
                .put("data", new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF});

        // Create record
        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, schema, value
        );

        // Apply transformation
        BytesToHexString.Value<SourceRecord> transform = new BytesToHexString.Value<>();
        transform.configure(Collections.emptyMap());

        SourceRecord transformed = transform.apply(record);

        // Verify schema changed: BYTES -> STRING
        Schema outputSchema = transformed.valueSchema();
        assertEquals(Schema.Type.STRING, outputSchema.field("data").schema().type());

        // Verify value converted to hex
        Struct outputValue = (Struct) transformed.value();
        assertEquals(123, outputValue.getInt32("id"));
        assertEquals("deadbeef", outputValue.getString("data"));

        transform.close();
    }

    /**
     * Example 2: Nested struct with multiple BYTES fields
     */
    @Test
    public void exampleNestedStructWithBytes() {
        // Create nested schema
        Schema innerSchema = SchemaBuilder.struct()
                .field("content", Schema.BYTES_SCHEMA)
                .field("metadata", Schema.STRING_SCHEMA)
                .build();

        Schema outerSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("header", Schema.BYTES_SCHEMA)
                .field("nested", innerSchema)
                .build();

        // Create nested value
        Struct innerValue = new Struct(innerSchema)
                .put("content", new byte[]{(byte) 0xCA, (byte) 0xFE})
                .put("metadata", "test");

        Struct outerValue = new Struct(outerSchema)
                .put("id", 456)
                .put("header", new byte[]{0x01, 0x02, 0x03})
                .put("nested", innerValue);

        // Create and transform record
        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, outerSchema, outerValue
        );

        BytesToHexString.Value<SourceRecord> transform = new BytesToHexString.Value<>();
        Map<String, Object> config = new HashMap<>();
        config.put("prefix", "0x");
        config.put("uppercase", true);
        transform.configure(config);

        SourceRecord transformed = transform.apply(record);

        // Verify nested conversion
        Struct outputValue = (Struct) transformed.value();
        assertEquals("0x010203", outputValue.getString("header"));

        Struct nestedOutput = outputValue.getStruct("nested");
        assertEquals("0xCAFE", nestedOutput.getString("content"));
        assertEquals("test", nestedOutput.getString("metadata"));

        transform.close();
    }

    /**
     * Example 3: Array of BYTES
     */
    @Test
    public void exampleArrayOfBytes() {
        // Create schema with array of BYTES
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("dataArray", SchemaBuilder.array(Schema.BYTES_SCHEMA).build())
                .build();

        // Create value with array
        List<byte[]> bytesList = Arrays.asList(
                new byte[]{0x01, 0x02},
                new byte[]{0x03, 0x04},
                new byte[]{0x05, 0x06}
        );

        Struct value = new Struct(schema)
                .put("id", 789)
                .put("dataArray", bytesList);

        // Create and transform record
        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, schema, value
        );

        BytesToHexString.Value<SourceRecord> transform = new BytesToHexString.Value<>();
        transform.configure(Collections.emptyMap());

        SourceRecord transformed = transform.apply(record);

        // Verify array elements converted
        Struct outputValue = (Struct) transformed.value();
        List<String> hexStrings = (List<String>) outputValue.get("dataArray");

        assertEquals(3, hexStrings.size());
        assertEquals("0102", hexStrings.get(0));
        assertEquals("0304", hexStrings.get(1));
        assertEquals("0506", hexStrings.get(2));

        transform.close();
    }

    /**
     * Example 4: Map with BYTES values
     */
    @Test
    public void exampleMapWithBytesValues() {
        // Create schema with map of String -> BYTES
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("dataMap", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BYTES_SCHEMA).build())
                .build();

        // Create value with map
        Map<String, byte[]> bytesMap = new HashMap<>();
        bytesMap.put("key1", new byte[]{(byte) 0xAA, (byte) 0xBB});
        bytesMap.put("key2", new byte[]{(byte) 0xCC, (byte) 0xDD});

        Struct value = new Struct(schema)
                .put("id", 999)
                .put("dataMap", bytesMap);

        // Create and transform record
        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, schema, value
        );

        BytesToHexString.Value<SourceRecord> transform = new BytesToHexString.Value<>();
        transform.configure(Collections.emptyMap());

        SourceRecord transformed = transform.apply(record);

        // Verify map values converted
        Struct outputValue = (Struct) transformed.value();
        Map<String, String> hexMap = (Map<String, String>) outputValue.get("dataMap");

        assertEquals("aabb", hexMap.get("key1"));
        assertEquals("ccdd", hexMap.get("key2"));

        transform.close();
    }

    /**
     * Example 5: ByteBuffer instead of byte array
     */
    @Test
    public void exampleByteBuffer() {
        // Create schema
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("buffer", Schema.BYTES_SCHEMA)
                .build();

        // Create value with ByteBuffer
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{0x11, 0x22, 0x33, 0x44});
        Struct value = new Struct(schema)
                .put("id", 111)
                .put("buffer", buffer);

        // Create and transform record
        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, schema, value
        );

        BytesToHexString.Value<SourceRecord> transform = new BytesToHexString.Value<>();
        transform.configure(Collections.emptyMap());

        SourceRecord transformed = transform.apply(record);

        // Verify conversion
        Struct outputValue = (Struct) transformed.value();
        assertEquals("11223344", outputValue.getString("buffer"));

        transform.close();
    }

    /**
     * Example 6: Optional BYTES field with null value
     */
    @Test
    public void exampleOptionalBytesWithNull() {
        // Create schema with optional BYTES field
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("data", SchemaBuilder.bytes().optional().build())
                .build();

        // Create value with null BYTES
        Struct value = new Struct(schema)
                .put("id", 222)
                .put("data", null);

        // Create and transform record
        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, schema, value
        );

        BytesToHexString.Value<SourceRecord> transform = new BytesToHexString.Value<>();
        transform.configure(Collections.emptyMap());

        SourceRecord transformed = transform.apply(record);

        // Verify null handling
        Struct outputValue = (Struct) transformed.value();
        assertEquals(222, outputValue.getInt32("id"));
        assertNull(outputValue.getString("data"));

        // Verify schema is still optional
        assertTrue(transformed.valueSchema().field("data").schema().isOptional());

        transform.close();
    }

    /**
     * Example 7: No BYTES fields - schema and value unchanged
     */
    @Test
    public void exampleNoBytesFields() {
        // Create schema with no BYTES fields
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();

        Struct value = new Struct(schema)
                .put("id", 333)
                .put("name", "test");

        // Create and transform record
        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, schema, value
        );

        BytesToHexString.Value<SourceRecord> transform = new BytesToHexString.Value<>();
        transform.configure(Collections.emptyMap());

        SourceRecord transformed = transform.apply(record);

        // Verify record returned unchanged (same instance)
        assertSame(record, transformed);

        transform.close();
    }

    /**
     * Example 8: Deeply nested structure
     */
    @Test
    public void exampleDeeplyNested() {
        // Level 3: innermost struct
        Schema level3Schema = SchemaBuilder.struct()
                .field("deepData", Schema.BYTES_SCHEMA)
                .build();

        // Level 2: middle struct
        Schema level2Schema = SchemaBuilder.struct()
                .field("level3", level3Schema)
                .field("middleData", Schema.BYTES_SCHEMA)
                .build();

        // Level 1: outer struct
        Schema level1Schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("level2", level2Schema)
                .field("outerData", Schema.BYTES_SCHEMA)
                .build();

        // Create deeply nested value
        Struct level3Value = new Struct(level3Schema)
                .put("deepData", new byte[]{0x11});

        Struct level2Value = new Struct(level2Schema)
                .put("level3", level3Value)
                .put("middleData", new byte[]{0x22});

        Struct level1Value = new Struct(level1Schema)
                .put("id", 444)
                .put("level2", level2Value)
                .put("outerData", new byte[]{0x33});

        // Create and transform record
        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, level1Schema, level1Value
        );

        BytesToHexString.Value<SourceRecord> transform = new BytesToHexString.Value<>();
        transform.configure(Collections.emptyMap());

        SourceRecord transformed = transform.apply(record);

        // Verify all levels converted
        Struct output = (Struct) transformed.value();
        assertEquals("33", output.getString("outerData"));

        Struct level2Output = output.getStruct("level2");
        assertEquals("22", level2Output.getString("middleData"));

        Struct level3Output = level2Output.getStruct("level3");
        assertEquals("11", level3Output.getString("deepData"));

        transform.close();
    }
}

