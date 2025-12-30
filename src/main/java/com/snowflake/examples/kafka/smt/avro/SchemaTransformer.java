package com.snowflake.examples.kafka.smt.avro;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Handles schema transformation: converts BYTES fields to STRING fields.
 * 
 * This class recursively walks through schemas and builds new schemas
 * where BYTES fields become STRING fields. Handles Decimal logical types
 * based on configuration.
 */
class SchemaTransformer {

    private final boolean convertDecimalsToString;

    public SchemaTransformer(boolean convertDecimalsToString) {
        this.convertDecimalsToString = convertDecimalsToString;
    }

    /**
     * Transform a schema, converting BYTES fields to STRING (except Decimal when configured).
     * Returns the original schema if no transformable BYTES fields are found.
     */
    public Schema transform(Schema schema) {
        if (schema == null) {
            return null;
        }

        switch (schema.type()) {
            case BYTES:
                return transformBytesSchema(schema);
            
            case STRUCT:
                return transformStruct(schema);
            
            case ARRAY:
                return transformArray(schema);
            
            case MAP:
                return transformMap(schema);
            
            default:
                // No transformation needed for primitives, strings, etc.
                return schema;
        }
    }

    /**
     * Transform BYTES schema - handle Decimal logical type or raw bytes.
     */
    private Schema transformBytesSchema(Schema bytesSchema) {
        // Check if this is a Decimal logical type
        if (isDecimalLogicalType(bytesSchema)) {
            if (convertDecimalsToString) {
                // Convert Decimal BYTES to STRING
                return transformBytesToString(bytesSchema);
            } else {
                // Skip - keep original BYTES schema
                return bytesSchema;
            }
        } else {
            // Raw bytes - always convert to STRING
            return transformBytesToString(bytesSchema);
        }
    }

    /**
     * Check if schema represents a Decimal logical type.
     */
    private boolean isDecimalLogicalType(Schema schema) {
        return schema.name() != null && schema.name().equals(Decimal.LOGICAL_NAME);
    }

    /**
     * Convert a BYTES schema to a STRING schema.
     * Preserves optionality and other metadata, but removes logical type information.
     */
    private Schema transformBytesToString(Schema bytesSchema) {
        SchemaBuilder builder = SchemaBuilder.string();
        
        // Copy metadata but NOT the name (which contains logical type info for Decimals)
        if (bytesSchema.version() != null) {
            builder.version(bytesSchema.version());
        }
        if (bytesSchema.doc() != null) {
            builder.doc(bytesSchema.doc());
        }
        if (bytesSchema.parameters() != null) {
            builder.parameters(bytesSchema.parameters());
        }
        if (bytesSchema.isOptional()) {
            builder.optional();
        }
        
        // Don't set a default value for converted fields
        
        return builder.build();
    }

    /**
     * Transform a STRUCT schema by recursively transforming each field.
     */
    private Schema transformStruct(Schema structSchema) {
        SchemaBuilder builder = SchemaBuilder.struct();
        copySchemaMetadata(structSchema, builder);
        
        boolean anyFieldChanged = false;
        
        // Transform each field
        for (Field field : structSchema.fields()) {
            Schema originalFieldSchema = field.schema();
            Schema transformedFieldSchema = transform(originalFieldSchema);
            
            builder.field(field.name(), transformedFieldSchema);
            
            // Track if any field actually changed
            if (transformedFieldSchema != originalFieldSchema) {
                anyFieldChanged = true;
            }
        }
        
        // If no fields changed, return original schema (optimization)
        if (!anyFieldChanged) {
            return structSchema;
        }
        
        return builder.build();
    }

    /**
     * Transform an ARRAY schema by transforming the element schema.
     */
    private Schema transformArray(Schema arraySchema) {
        Schema originalElementSchema = arraySchema.valueSchema();
        Schema transformedElementSchema = transform(originalElementSchema);
        
        // If element schema didn't change, return original
        if (transformedElementSchema == originalElementSchema) {
            return arraySchema;
        }
        
        SchemaBuilder builder = SchemaBuilder.array(transformedElementSchema);
        copySchemaMetadata(arraySchema, builder);
        
        return builder.build();
    }

    /**
     * Transform a MAP schema by transforming the value schema.
     * Map keys are not transformed (typically STRING or primitives).
     */
    private Schema transformMap(Schema mapSchema) {
        Schema keySchema = mapSchema.keySchema();
        Schema originalValueSchema = mapSchema.valueSchema();
        Schema transformedValueSchema = transform(originalValueSchema);
        
        // If value schema didn't change, return original
        if (transformedValueSchema == originalValueSchema) {
            return mapSchema;
        }
        
        SchemaBuilder builder = SchemaBuilder.map(keySchema, transformedValueSchema);
        copySchemaMetadata(mapSchema, builder);
        
        return builder.build();
    }

    /**
     * Copy common schema metadata (name, version, doc, parameters, optionality).
     */
    private void copySchemaMetadata(Schema source, SchemaBuilder target) {
        if (source.name() != null) {
            target.name(source.name());
        }
        if (source.version() != null) {
            target.version(source.version());
        }
        if (source.doc() != null) {
            target.doc(source.doc());
        }
        if (source.parameters() != null) {
            target.parameters(source.parameters());
        }
        if (source.isOptional()) {
            target.optional();
        }
    }
}

