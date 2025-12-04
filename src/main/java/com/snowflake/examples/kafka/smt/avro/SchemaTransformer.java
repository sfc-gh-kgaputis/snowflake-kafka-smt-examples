package com.snowflake.examples.kafka.smt.avro;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Handles schema transformation: converts BYTES fields to STRING fields.
 * 
 * This class recursively walks through schemas and builds new schemas
 * where every BYTES field becomes a STRING field.
 */
class SchemaTransformer {

    /**
     * Transform a schema, converting all BYTES fields to STRING.
     * Returns the original schema if no BYTES fields are found.
     */
    public Schema transform(Schema schema) {
        if (schema == null) {
            return null;
        }

        switch (schema.type()) {
            case BYTES:
                return transformBytesToString(schema);
            
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
     * Convert a BYTES schema to a STRING schema.
     * Preserves optionality and other metadata.
     */
    private Schema transformBytesToString(Schema bytesSchema) {
        SchemaBuilder builder = SchemaBuilder.string();
        
        copySchemaMetadata(bytesSchema, builder);
        
        // Convert default value if present
        if (bytesSchema.defaultValue() != null) {
            // Note: We can't convert bytes to hex here without the config,
            // so we skip default value conversion. In practice, BYTES fields
            // rarely have default values.
            builder.defaultValue(null);
        }
        
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

