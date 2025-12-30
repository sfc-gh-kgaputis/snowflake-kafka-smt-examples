package com.snowflake.examples.kafka.smt.avro;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
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

    /**
     * Test 12: Decimal logical type - convert to string (default)
     */
    @Test
    public void testDecimalConvertToString() {
        Schema decimalSchema = Decimal.schema(2);  // scale=2
        
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("amount", decimalSchema)
                .field("price", decimalSchema)
                .build();

        Struct value = new Struct(schema)
                .put("id", 123)
                .put("amount", new BigDecimal("123.45"))
                .put("price", new BigDecimal("5.00"));

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, schema, value
        );

        BytesToEncodedString.Value<SourceRecord> transform = new BytesToEncodedString.Value<>();
        Map<String, Object> config = new HashMap<>();
        config.put("convertDecimalsToString", true);
        transform.configure(config);

        SourceRecord transformed = transform.apply(record);

        // Verify decimals converted to string
        Struct outputValue = (Struct) transformed.value();
        assertEquals(123, outputValue.getInt32("id"));
        assertEquals("123.45", outputValue.getString("amount"));
        assertEquals("5.00", outputValue.getString("price"));

        // Verify schema changed from BYTES to STRING
        Schema outputSchema = transformed.valueSchema();
        assertEquals(Schema.Type.STRING, outputSchema.field("amount").schema().type());
        assertEquals(Schema.Type.STRING, outputSchema.field("price").schema().type());

        transform.close();
    }

    /**
     * Test 13: Decimal logical type - skip conversion (convertDecimalsToString=false)
     */
    @Test
    public void testDecimalSkipConversion() {
        Schema decimalSchema = Decimal.schema(2);  // scale=2
        
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("amount", decimalSchema)
                .build();

        BigDecimal originalAmount = new BigDecimal("123.45");
        Struct value = new Struct(schema)
                .put("id", 456)
                .put("amount", originalAmount);

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, schema, value
        );

        BytesToEncodedString.Value<SourceRecord> transform = new BytesToEncodedString.Value<>();
        Map<String, Object> config = new HashMap<>();
        config.put("convertDecimalsToString", false);
        transform.configure(config);

        SourceRecord transformed = transform.apply(record);

        // Verify decimals NOT converted - should be unchanged
        Struct outputValue = (Struct) transformed.value();
        assertEquals(456, outputValue.getInt32("id"));
        
        // Value should still be BigDecimal
        Object amountValue = outputValue.get("amount");
        assertTrue(amountValue instanceof BigDecimal);
        assertEquals(originalAmount, amountValue);

        // Schema should still be BYTES with Decimal logical type
        Schema outputSchema = transformed.valueSchema();
        Schema amountSchema = outputSchema.field("amount").schema();
        assertEquals(Schema.Type.BYTES, amountSchema.type());
        assertEquals(Decimal.LOGICAL_NAME, amountSchema.name());

        transform.close();
    }

    /**
     * Test 14: Mixed bytes and decimals - convertDecimalsToString=true
     */
    @Test
    public void testMixedBytesAndDecimalsConvertTrue() {
        Schema decimalSchema = Decimal.schema(2);
        
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("binaryData", Schema.BYTES_SCHEMA)  // Raw bytes
                .field("price", decimalSchema)             // Decimal
                .build();

        Struct value = new Struct(schema)
                .put("id", 789)
                .put("binaryData", new byte[]{(byte) 0xAA, (byte) 0xBB})
                .put("price", new BigDecimal("99.99"));

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, schema, value
        );

        BytesToEncodedString.Value<SourceRecord> transform = new BytesToEncodedString.Value<>();
        Map<String, Object> config = new HashMap<>();
        config.put("encoding", "base64");
        config.put("convertDecimalsToString", true);
        transform.configure(config);

        SourceRecord transformed = transform.apply(record);

        // Verify both converted to STRING
        Struct outputValue = (Struct) transformed.value();
        assertEquals(789, outputValue.getInt32("id"));
        assertEquals("qrs=", outputValue.getString("binaryData"));  // base64 encoded
        assertEquals("99.99", outputValue.getString("price"));      // decimal as string

        // Both should be STRING in schema
        Schema outputSchema = transformed.valueSchema();
        assertEquals(Schema.Type.STRING, outputSchema.field("binaryData").schema().type());
        assertEquals(Schema.Type.STRING, outputSchema.field("price").schema().type());

        transform.close();
    }

    /**
     * Test 15: Mixed bytes and decimals - convertDecimalsToString=false
     */
    @Test
    public void testMixedBytesAndDecimalsConvertFalse() {
        Schema decimalSchema = Decimal.schema(2);
        
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("binaryData", Schema.BYTES_SCHEMA)  // Raw bytes
                .field("price", decimalSchema)             // Decimal
                .build();

        BigDecimal originalPrice = new BigDecimal("99.99");
        Struct value = new Struct(schema)
                .put("id", 789)
                .put("binaryData", new byte[]{(byte) 0xAA, (byte) 0xBB})
                .put("price", originalPrice);

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, schema, value
        );

        BytesToEncodedString.Value<SourceRecord> transform = new BytesToEncodedString.Value<>();
        Map<String, Object> config = new HashMap<>();
        config.put("encoding", "hex");
        config.put("convertDecimalsToString", false);
        transform.configure(config);

        SourceRecord transformed = transform.apply(record);

        // Verify: raw bytes converted, decimal unchanged
        Struct outputValue = (Struct) transformed.value();
        assertEquals(789, outputValue.getInt32("id"));
        assertEquals("aabb", outputValue.getString("binaryData"));  // hex encoded
        
        Object priceValue = outputValue.get("price");
        assertTrue(priceValue instanceof BigDecimal);
        assertEquals(originalPrice, priceValue);

        // Raw bytes should be STRING, decimal should still be BYTES
        Schema outputSchema = transformed.valueSchema();
        assertEquals(Schema.Type.STRING, outputSchema.field("binaryData").schema().type());
        assertEquals(Schema.Type.BYTES, outputSchema.field("price").schema().type());

        transform.close();
    }

    /**
     * Test 16: Nested struct with decimal
     */
    @Test
    public void testNestedStructWithDecimal() {
        Schema decimalSchema = Decimal.schema(4);
        
        Schema innerSchema = SchemaBuilder.struct()
                .field("amount", decimalSchema)
                .field("currency", Schema.STRING_SCHEMA)
                .build();

        Schema outerSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("transaction", innerSchema)
                .build();

        Struct innerValue = new Struct(innerSchema)
                .put("amount", new BigDecimal("1234.5678"))
                .put("currency", "USD");

        Struct outerValue = new Struct(outerSchema)
                .put("id", 999)
                .put("transaction", innerValue);

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, outerSchema, outerValue
        );

        BytesToEncodedString.Value<SourceRecord> transform = new BytesToEncodedString.Value<>();
        Map<String, Object> config = new HashMap<>();
        config.put("convertDecimalsToString", true);
        transform.configure(config);

        SourceRecord transformed = transform.apply(record);

        // Verify nested decimal converted
        Struct outputValue = (Struct) transformed.value();
        Struct transactionOutput = outputValue.getStruct("transaction");
        assertEquals("1234.5678", transactionOutput.getString("amount"));
        assertEquals("USD", transactionOutput.getString("currency"));

        transform.close();
    }

    /**
     * Test 17: Decimal with large precision (Debezium scenario) - opt-in conversion
     */
    @Test
    public void testDebeziumDecimalWithConversion() {
        // Debezium uses scale=38, precision defined in parameters
        Schema decimalSchema = Decimal.builder(38).parameter("scale", "9").build();
        
        Schema schema = SchemaBuilder.struct()
                .field("FILTERINFOID", decimalSchema)
                .field("NAME", Schema.STRING_SCHEMA)
                .build();

        Struct value = new Struct(schema)
                .put("FILTERINFOID", new BigDecimal("123456789.123456789"))
                .put("NAME", "TestRecord");

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, schema, value
        );

        BytesToEncodedString.Value<SourceRecord> transform = new BytesToEncodedString.Value<>();
        Map<String, Object> config = new HashMap<>();
        config.put("convertDecimalsToString", true);  // Opt-in to convert decimals
        transform.configure(config);

        SourceRecord transformed = transform.apply(record);

        // Verify Debezium decimal converted to string
        Struct outputValue = (Struct) transformed.value();
        assertEquals("123456789.123456789", outputValue.getString("FILTERINFOID"));
        assertEquals("TestRecord", outputValue.getString("NAME"));

        // Schema should be STRING
        Schema outputSchema = transformed.valueSchema();
        assertEquals(Schema.Type.STRING, outputSchema.field("FILTERINFOID").schema().type());

        transform.close();
    }

    /**
     * Test 18: Default behavior - decimals are NOT converted
     */
    @Test
    public void testDefaultDecimalBehavior() {
        Schema decimalSchema = Decimal.schema(2);
        
        Schema schema = SchemaBuilder.struct()
                .field("amount", decimalSchema)
                .field("rawBytes", Schema.BYTES_SCHEMA)
                .build();

        BigDecimal originalAmount = new BigDecimal("99.99");
        Struct value = new Struct(schema)
                .put("amount", originalAmount)
                .put("rawBytes", new byte[]{0x01, 0x02});

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", 0, schema, value
        );

        BytesToEncodedString.Value<SourceRecord> transform = new BytesToEncodedString.Value<>();
        transform.configure(Collections.emptyMap());  // Uses default: convertDecimalsToString=false

        SourceRecord transformed = transform.apply(record);

        // Verify: decimal unchanged, raw bytes encoded
        Struct outputValue = (Struct) transformed.value();
        
        // Decimal should remain as BigDecimal
        Object amountValue = outputValue.get("amount");
        assertTrue(amountValue instanceof BigDecimal);
        assertEquals(originalAmount, amountValue);
        
        // Raw bytes should be encoded (base64 by default)
        assertEquals("AQI=", outputValue.getString("rawBytes"));

        // Schema: decimal stays BYTES, raw bytes becomes STRING
        Schema outputSchema = transformed.valueSchema();
        assertEquals(Schema.Type.BYTES, outputSchema.field("amount").schema().type());
        assertEquals(Schema.Type.STRING, outputSchema.field("rawBytes").schema().type());

        transform.close();
    }
}

