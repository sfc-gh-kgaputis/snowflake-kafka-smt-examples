# Snowflake Kafka Custom SMT Examples

**PLEASE NOTE:** This example project is not an official Snowflake offering. It comes with no support or warranty.

## Available Transformations

### BytesToHexString

**Package:** `com.snowflake.examples.kafka.smt.avro`

Converts Avro `BYTES` fields to hex-encoded strings in deeply nested schemas. Works recursively through Structs, Arrays, and Maps.

**Configuration:**
```properties
transforms=bytesToHex
transforms.bytesToHex.type=com.snowflake.examples.kafka.smt.avro.BytesToHexString$Value
transforms.bytesToHex.prefix=0x              # Optional prefix (default: "")
transforms.bytesToHex.uppercase=true         # Use A-F vs a-f (default: false)
transforms.bytesToHex.storeAsVarchar=true    # Transform schema to STRING (default: true)
```

**Options:**
- `storeAsVarchar=true` (default): Converts both schema and values (BYTES→STRING)
- `storeAsVarchar=false`: Keeps BYTES schema but converts values to hex strings (Snowflake-compatible)

Use `$Key` for keys or `$Value` for values.

### AddKafkaMetadataColumns

Adds Kafka metadata (topic, partition, offset) as top-level columns in schemaless JSON records. Useful for efficient query pruning when schematization is enabled.

**Configuration:**
```properties
transforms.addMeta.type=com.snowflake.examples.kafka.smt.AddKafkaMetadataColumns
transforms.addMeta.columnNameKafkaTopic=kafka_topic
transforms.addMeta.columnNameKafkaPartition=kafka_partition
transforms.addMeta.columnNameKafkaOffset=kafka_offset
```

**Note:** Duplicates metadata already in `RECORD_METADATA` variant column.

### AddSchemaIdHeader

Adds Avro schema version to message headers as `schema_id`. The header appears in Snowflake's `RECORD_METADATA` column.

**Configuration:**
```properties
transforms.schemaId.type=com.snowflake.examples.kafka.smt.AddSchemaIdHeader
```

**Requirements:** Record must have a schema with a version number.

### LogIngestMetrics

Logs Snowflake Streaming Ingest SDK JMX metrics every 30 seconds. Useful for monitoring pipeline performance.

**Configuration:**
```properties
transforms.metrics.type=com.snowflake.examples.kafka.smt.LogIngestMetrics
```

**Note:** Does not modify records. Purely observability-focused.

### ReshapeVehicleEvent

Example transformation showing partial schematization pattern. Validates and extracts required fields (`timestamp`, `vin`, `type`) to top-level, nests remaining fields in `payload` object.

**Configuration:**
```properties
transforms.reshape.type=com.snowflake.examples.kafka.smt.ReshapeVehicleEvent
```

**Note:** Throws `DataException` on missing required fields. Configure dead letter queue or expect task failures.

## Prerequisites

### Java 11+

Build target is Java 11.

### Maven

Required for building.

## Download

**Latest Release:**

Download the pre-built shaded JAR from [GitHub Releases](../../releases/latest).

## Build and Deploy

**Build from source:**
```bash
mvn clean package
```

Output: `target/snowflake-kafka-smt-examples-1.0-SNAPSHOT-shaded.jar`

**Deploy:**

Add the shaded JAR to your Kafka Connect worker classpath (e.g., `/opt/kafka/libs/` or plugin path).

## Usage Example

Example Snowflake Sink connector with multiple transforms:

```json
{
  "name": "snowflake-sink-example",
  "config": {
    "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
    "snowflake.ingestion.method": "SNOWPIPE_STREAMING",
    "tasks.max": "1",
    "topics": "my_topic",
    "snowflake.enable.schematization": "true",
    "snowflake.url.name": "myaccount.snowflakecomputing.com:443",
    "snowflake.user.name": "kafka_user",
    "snowflake.private.key": "...",
    "snowflake.database.name": "mydb",
    "snowflake.schema.name": "public",
    
    "transforms": "bytesToHex,addMeta,metrics",
    "transforms.bytesToHex.type": "com.snowflake.examples.kafka.smt.avro.BytesToHexString$Value",
    "transforms.bytesToHex.prefix": "0x",
    "transforms.addMeta.type": "com.snowflake.examples.kafka.smt.AddKafkaMetadataColumns",
    "transforms.addMeta.columnNameKafkaTopic": "kafka_topic",
    "transforms.addMeta.columnNameKafkaPartition": "kafka_partition",
    "transforms.addMeta.columnNameKafkaOffset": "kafka_offset",
    "transforms.metrics.type": "com.snowflake.examples.kafka.smt.LogIngestMetrics"
  }
}
```