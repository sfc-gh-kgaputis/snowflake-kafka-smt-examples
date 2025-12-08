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
 * Test cases for BytesToEncodedString transformation with both hex and base64 encoding.
 */
public class BytesToEncodedStringTest {

    /**
     * Test 1: Simple bytes field with base64 encoding (recommended)
     */
    @Test
    public void testSimpleBytesFieldBase64() {
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("data", Schema.BYTES_SCHEMA)
                .build();

        Struct value = new Struct(schema)
                .put("id", 123)
                .put("data", new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF});

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, schema, value
        );

        BytesToEncodedString.Value<SourceRecord> transform = new BytesToEncodedString.Value<>();
        Map<String, Object> config = new HashMap<>();
        config.put("encoding", "base64");
        transform.configure(config);

        SourceRecord transformed = transform.apply(record);

        // Verify schema changed: BYTES -> STRING
        Schema outputSchema = transformed.valueSchema();
        assertEquals(Schema.Type.STRING, outputSchema.field("data").schema().type());

        // Verify value converted to base64
        Struct outputValue = (Struct) transformed.value();
        assertEquals(123, outputValue.getInt32("id"));
        assertEquals("3q2+7w==", outputValue.getString("data"));

        transform.close();
    }

    /**
     * Test 2: Simple bytes field with hex encoding
     */
    @Test
    public void testSimpleBytesFieldHex() {
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("data", Schema.BYTES_SCHEMA)
                .build();

        Struct value = new Struct(schema)
                .put("id", 123)
                .put("data", new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF});

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, schema, value
        );

        BytesToEncodedString.Value<SourceRecord> transform = new BytesToEncodedString.Value<>();
        Map<String, Object> config = new HashMap<>();
        config.put("encoding", "hex");
        transform.configure(config);

        SourceRecord transformed = transform.apply(record);

        // Verify value converted to hex
        Struct outputValue = (Struct) transformed.value();
        assertEquals(123, outputValue.getInt32("id"));
        assertEquals("deadbeef", outputValue.getString("data"));

        transform.close();
    }

    /**
     * Test 3: Nested struct with base64 encoding
     */
    @Test
    public void testNestedStructBase64() {
        Schema innerSchema = SchemaBuilder.struct()
                .field("content", Schema.BYTES_SCHEMA)
                .field("metadata", Schema.STRING_SCHEMA)
                .build();

        Schema outerSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("header", Schema.BYTES_SCHEMA)
                .field("nested", innerSchema)
                .build();

        Struct innerValue = new Struct(innerSchema)
                .put("content", new byte[]{(byte) 0xCA, (byte) 0xFE})
                .put("metadata", "test");

        Struct outerValue = new Struct(outerSchema)
                .put("id", 456)
                .put("header", new byte[]{0x01, 0x02, 0x03})
                .put("nested", innerValue);

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, outerSchema, outerValue
        );

        BytesToEncodedString.Value<SourceRecord> transform = new BytesToEncodedString.Value<>();
        Map<String, Object> config = new HashMap<>();
        config.put("encoding", "base64");
        transform.configure(config);

        SourceRecord transformed = transform.apply(record);

        // Verify nested conversion
        Struct outputValue = (Struct) transformed.value();
        assertEquals("AQID", outputValue.getString("header"));

        Struct nestedOutput = outputValue.getStruct("nested");
        assertEquals("yv4=", nestedOutput.getString("content"));
        assertEquals("test", nestedOutput.getString("metadata"));

        transform.close();
    }

    /**
     * Test 4: Nested struct with hex encoding and uppercase
     */
    @Test
    public void testNestedStructHexUppercase() {
        Schema innerSchema = SchemaBuilder.struct()
                .field("content", Schema.BYTES_SCHEMA)
                .field("metadata", Schema.STRING_SCHEMA)
                .build();

        Schema outerSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("header", Schema.BYTES_SCHEMA)
                .field("nested", innerSchema)
                .build();

        Struct innerValue = new Struct(innerSchema)
                .put("content", new byte[]{(byte) 0xCA, (byte) 0xFE})
                .put("metadata", "test");

        Struct outerValue = new Struct(outerSchema)
                .put("id", 456)
                .put("header", new byte[]{0x01, 0x02, 0x03})
                .put("nested", innerValue);

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, outerSchema, outerValue
        );

        BytesToEncodedString.Value<SourceRecord> transform = new BytesToEncodedString.Value<>();
        Map<String, Object> config = new HashMap<>();
        config.put("encoding", "hex");
        config.put("prefix", "0x");  // Note: For demo only - not recommended for Snowflake
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
     * Test 5: Array of bytes with base64
     */
    @Test
    public void testArrayOfBytesBase64() {
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("dataArray", SchemaBuilder.array(Schema.BYTES_SCHEMA).build())
                .build();

        List<byte[]> bytesList = Arrays.asList(
                new byte[]{0x01, 0x02},
                new byte[]{0x03, 0x04},
                new byte[]{0x05, 0x06}
        );

        Struct value = new Struct(schema)
                .put("id", 789)
                .put("dataArray", bytesList);

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, schema, value
        );

        BytesToEncodedString.Value<SourceRecord> transform = new BytesToEncodedString.Value<>();
        Map<String, Object> config = new HashMap<>();
        config.put("encoding", "base64");
        transform.configure(config);

        SourceRecord transformed = transform.apply(record);

        // Verify array elements converted
        Struct outputValue = (Struct) transformed.value();
        List<String> encodedStrings = (List<String>) outputValue.get("dataArray");

        assertEquals(3, encodedStrings.size());
        assertEquals("AQI=", encodedStrings.get(0));
        assertEquals("AwQ=", encodedStrings.get(1));
        assertEquals("BQY=", encodedStrings.get(2));

        transform.close();
    }

    /**
     * Test 6: Array of bytes with hex
     */
    @Test
    public void testArrayOfBytesHex() {
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("dataArray", SchemaBuilder.array(Schema.BYTES_SCHEMA).build())
                .build();

        List<byte[]> bytesList = Arrays.asList(
                new byte[]{0x01, 0x02},
                new byte[]{0x03, 0x04},
                new byte[]{0x05, 0x06}
        );

        Struct value = new Struct(schema)
                .put("id", 789)
                .put("dataArray", bytesList);

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, schema, value
        );

        BytesToEncodedString.Value<SourceRecord> transform = new BytesToEncodedString.Value<>();
        Map<String, Object> config = new HashMap<>();
        config.put("encoding", "hex");
        transform.configure(config);

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
     * Test 7: Map with bytes values - base64
     */
    @Test
    public void testMapWithBytesValuesBase64() {
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("dataMap", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BYTES_SCHEMA).build())
                .build();

        Map<String, byte[]> bytesMap = new HashMap<>();
        bytesMap.put("key1", new byte[]{(byte) 0xAA, (byte) 0xBB});
        bytesMap.put("key2", new byte[]{(byte) 0xCC, (byte) 0xDD});

        Struct value = new Struct(schema)
                .put("id", 999)
                .put("dataMap", bytesMap);

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, schema, value
        );

        BytesToEncodedString.Value<SourceRecord> transform = new BytesToEncodedString.Value<>();
        Map<String, Object> config = new HashMap<>();
        config.put("encoding", "base64");
        transform.configure(config);

        SourceRecord transformed = transform.apply(record);

        // Verify map values converted
        Struct outputValue = (Struct) transformed.value();
        Map<String, String> encodedMap = (Map<String, String>) outputValue.get("dataMap");

        assertEquals("qrs=", encodedMap.get("key1"));
        assertEquals("zN0=", encodedMap.get("key2"));

        transform.close();
    }

    /**
     * Test 8: ByteBuffer with base64
     */
    @Test
    public void testByteBufferBase64() {
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("buffer", Schema.BYTES_SCHEMA)
                .build();

        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{0x11, 0x22, 0x33, 0x44});
        Struct value = new Struct(schema)
                .put("id", 111)
                .put("buffer", buffer);

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, schema, value
        );

        BytesToEncodedString.Value<SourceRecord> transform = new BytesToEncodedString.Value<>();
        Map<String, Object> config = new HashMap<>();
        config.put("encoding", "base64");
        transform.configure(config);

        SourceRecord transformed = transform.apply(record);

        // Verify conversion
        Struct outputValue = (Struct) transformed.value();
        assertEquals("ESIzRA==", outputValue.getString("buffer"));

        transform.close();
    }

    /**
     * Test 9: Optional bytes field with null value
     */
    @Test
    public void testOptionalBytesWithNull() {
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("data", SchemaBuilder.bytes().optional().build())
                .build();

        Struct value = new Struct(schema)
                .put("id", 222)
                .put("data", null);

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, schema, value
        );

        BytesToEncodedString.Value<SourceRecord> transform = new BytesToEncodedString.Value<>();
        transform.configure(Collections.emptyMap());  // Uses default base64

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
     * Test 10: No bytes fields - schema and value unchanged
     */
    @Test
    public void testNoBytesFields() {
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();

        Struct value = new Struct(schema)
                .put("id", 333)
                .put("name", "test");

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, schema, value
        );

        BytesToEncodedString.Value<SourceRecord> transform = new BytesToEncodedString.Value<>();
        transform.configure(Collections.emptyMap());  // Uses default base64

        SourceRecord transformed = transform.apply(record);

        // Verify record returned unchanged (same instance)
        assertSame(record, transformed);

        transform.close();
    }

    /**
     * Test 11: Default encoding should be base64
     */
    @Test
    public void testDefaultEncodingIsBase64() {
        Schema schema = SchemaBuilder.struct()
                .field("data", Schema.BYTES_SCHEMA)
                .build();

        Struct value = new Struct(schema)
                .put("data", new byte[]{0x01, 0x02, 0x03});

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, schema, value
        );

        BytesToEncodedString.Value<SourceRecord> transform = new BytesToEncodedString.Value<>();
        transform.configure(Collections.emptyMap());  // No encoding specified

        SourceRecord transformed = transform.apply(record);

        // Verify it used base64 (not hex)
        Struct outputValue = (Struct) transformed.value();
        assertEquals("AQID", outputValue.getString("data"));  // base64 encoding

        transform.close();
    }
}

