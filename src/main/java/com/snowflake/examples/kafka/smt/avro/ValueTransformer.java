package com.snowflake.examples.kafka.smt.avro;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles value transformation: converts byte arrays to hex strings.
 * 
 * This class recursively walks through values and converts all byte arrays
 * (or ByteBuffers) to hex-encoded strings based on the schema.
 */
class ValueTransformer {

    private final HexConverter hexConverter;

    public ValueTransformer(HexConverter hexConverter) {
        this.hexConverter = hexConverter;
    }

    /**
     * Transform a value based on its schema.
     * Converts all BYTES values to hex strings.
     */
    public Object transform(Object value, Schema schema) {
        if (value == null) {
            return null;
        }

        switch (schema.type()) {
            case BYTES:
                return transformBytesToHex(value);
            
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
     * Convert bytes (byte[] or ByteBuffer) to hex string.
     */
    private String transformBytesToHex(Object bytesValue) {
        byte[] bytes = extractBytes(bytesValue);
        return hexConverter.toHex(bytes);
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
        SchemaTransformer schemaTransformer = new SchemaTransformer();
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

