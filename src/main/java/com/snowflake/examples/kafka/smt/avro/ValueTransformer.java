package com.snowflake.examples.kafka.smt.avro;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles value transformation: converts byte arrays to encoded strings.
 * 
 * This class recursively walks through values and converts all byte arrays
 * (or ByteBuffers) to encoded strings based on the schema. Handles Decimal
 * logical types specially.
 */
class ValueTransformer {

    private final ByteEncoder byteEncoder;
    private final boolean convertDecimalsToString;

    public ValueTransformer(ByteEncoder byteEncoder, boolean convertDecimalsToString) {
        this.byteEncoder = byteEncoder;
        this.convertDecimalsToString = convertDecimalsToString;
    }

    /**
     * Transform a value based on its schema.
     * Converts BYTES values to encoded strings, handles Decimal logical types.
     */
    public Object transform(Object value, Schema schema) {
        if (value == null) {
            return null;
        }

        switch (schema.type()) {
            case BYTES:
                return transformBytes(value, schema);
            
            case STRUCT:
                return transformStruct((Struct) value, schema);
            
            case ARRAY:
                return transformArray((List<?>) value, schema);
            
            case MAP:
                return transformMap((Map<?, ?>) value, schema);
            
            default:
                // No transformation for primitives, strings, etc.
                return value;
        }
    }

    /**
     * Transform BYTES field - handle Decimal logical type or raw bytes.
     */
    private Object transformBytes(Object value, Schema schema) {
        // Check if this is a Decimal logical type
        if (isDecimalLogicalType(schema)) {
            if (convertDecimalsToString) {
                // Convert BigDecimal to string
                return decimalToString(value);
            } else {
                // Skip - return unchanged
                return value;
            }
        } else {
            // Raw bytes - encode to string
            return transformBytesToEncoded(value);
        }
    }

    /**
     * Check if schema represents a Decimal logical type.
     */
    private boolean isDecimalLogicalType(Schema schema) {
        return schema.name() != null && schema.name().equals(Decimal.LOGICAL_NAME);
    }

    /**
     * Convert Decimal value (BigDecimal) to plain string.
     */
    private String decimalToString(Object value) {
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).toPlainString();
        }
        throw new DataException(
            "Expected BigDecimal for Decimal logical type, but got: " + value.getClass()
        );
    }

    /**
     * Convert bytes (byte[] or ByteBuffer) to encoded string.
     */
    private String transformBytesToEncoded(Object bytesValue) {
        byte[] bytes = extractBytes(bytesValue);
        return byteEncoder.encode(bytes);
    }

    /**
     * Extract byte array from byte[] or ByteBuffer.
     */
    private byte[] extractBytes(Object bytesValue) {
        if (bytesValue instanceof byte[]) {
            return (byte[]) bytesValue;
        }
        
        if (bytesValue instanceof ByteBuffer) {
            ByteBuffer buffer = (ByteBuffer) bytesValue;
            byte[] bytes = new byte[buffer.remaining()];
            buffer.duplicate().get(bytes); // Use duplicate to avoid modifying position
            return bytes;
        }
        
        throw new DataException(
            "Expected byte[] or ByteBuffer for BYTES field, but got: " + bytesValue.getClass()
        );
    }

    /**
     * Transform a STRUCT value by recursively transforming each field.
     * Note: The transformedSchema parameter is expected to be passed in by the caller.
     */
    public Struct transformStruct(Struct originalStruct, Schema originalSchema, Schema transformedSchema) {
        Struct transformedStruct = new Struct(transformedSchema);
        
        for (Field field : originalSchema.fields()) {
            Object fieldValue = originalStruct.get(field);
            Object transformedValue = transform(fieldValue, field.schema());
            transformedStruct.put(field.name(), transformedValue);
        }
        
        return transformedStruct;
    }

    /**
     * Transform a STRUCT value (for internal recursion).
     */
    private Struct transformStruct(Struct originalStruct, Schema originalSchema) {
        // For nested structs, we need to rebuild the schema on the fly
        // This is less efficient but keeps the API simple for recursive calls
        SchemaTransformer schemaTransformer = new SchemaTransformer(convertDecimalsToString);
        Schema transformedSchema = schemaTransformer.transform(originalSchema);
        return transformStruct(originalStruct, originalSchema, transformedSchema);
    }

    /**
     * Transform an ARRAY value by recursively transforming each element.
     */
    private List<Object> transformArray(List<?> originalList, Schema arraySchema) {
        Schema elementSchema = arraySchema.valueSchema();
        List<Object> transformedList = new ArrayList<>(originalList.size());
        
        for (Object element : originalList) {
            Object transformedElement = transform(element, elementSchema);
            transformedList.add(transformedElement);
        }
        
        return transformedList;
    }

    /**
     * Transform a MAP value by recursively transforming each value.
     * Map keys are not transformed.
     */
    private Map<Object, Object> transformMap(Map<?, ?> originalMap, Schema mapSchema) {
        Schema valueSchema = mapSchema.valueSchema();
        Map<Object, Object> transformedMap = new HashMap<>(originalMap.size());
        
        for (Map.Entry<?, ?> entry : originalMap.entrySet()) {
            Object transformedValue = transform(entry.getValue(), valueSchema);
            transformedMap.put(entry.getKey(), transformedValue);
        }
        
        return transformedMap;
    }
}

