# Snowflake Kafka Custom SMT Examples

**PLEASE NOTE:** This example project is not an official Snowflake offering. It comes with no support or warranty.

## Included Examples

Currently, the following examples are included in this repo:

### AddKafkaMetadataColumns

This transform will populate top-level columns with Kafka topic, Kafka partition number and Kafka offset metadata, assuming that schematization is enabled in the Kafka Connector.  

This duplicates Kafka metadata which is already persisted in the RECORD_METADATA variant, but may be useful to enable more efficient monitoring and query pruning in certain scenarios. 

Please use the following config to enable this transform. The column names are customizable based on your requirements.
```
"transforms.addKafkaMetadataColumns.type": "com.snowflake.examples.kafka.smt.AddKafkaMetadataColumns",
"transforms.addKafkaMetadataColumns.columnNameKafkaTopic": "custom$kafka_topic",
"transforms.addKafkaMetadataColumns.columnNameKafkaPartition": "custom$kafka_partition",
"transforms.addKafkaMetadataColumns.columnNameKafkaOffset": "custom$kafka_offset",
```
***Please Note:*** These columns will be created automatically when schematization is enabled. 

### AddSinkTimestampHeader

This transform will add the system time (in millis) of the Kafka Connect worker to the Kafka message headers. This
timestamp will then be present in the RECORD_METADATA variant column in Snowflake.

### ParseAndFlattenXml

This transform will take flat XML (no attributes, no deeply nested child elements) and parse it into a tabular structure. Please use in conjunction with a String or ByteArray value converter.

***Please Note:*** You will notice that ParseAndFlattenXml throws a `DataException` in the case of a validation error.
When this occurs, you will need to either configure a dead letter queue to accept error records, or this exception could
cause the Kafka Connect task to fail and stop. Either of these may be the desired case if you want to avoid data loss.

### ReshapeVehicleEvent

This transform will reshape JSON messages for a fictitious vehicle event stream use case. It will check for several
required fields, which will be kept in the top level of each record, and then all remaining fields will be nested in a
variant column called `PAYLOAD`.

This pattern could be useful if you are ingesting multiple event types into the same table, but you only want partial
schematization.

***Please Note:*** You will notice that ReshapeVehicleEvent throws a `DataException` in the case of a validation error.
When this occurs, you will need to either configure a dead letter queue to accept error records, or this exception could
cause the Kafka Connect task to fail and stop. Either of these may be the desired case if you want to avoid data loss.

## Prerequisites

### Java 8+

This code assumes Java 8 or higher. The current build target is Java 8, although this can be changed.

### Maven (Developers only)

These examples are packaged in a Maven project, so you will need Maven to load dependencies, and then compile and
package the code.

## Build and deploy JAR to Kafka Connect

First you want to build the JAR which contains the custom SMT transforms.

```
mvn package
```

The JAR file output can then be found in the `target/` folder.

Alternatively, a snapshot build of this JAR has been included in the `dist/` folder of this repo.

This JAR will need to be added to the classpath of your Kafka Connect workers. One approach is to put the JAR in
the `libs/` folder of Kafka. For example, this could be: `/opt/kafka/libs`.

## Include SMT transforms in your Snowflake sink

Here is an example connector JSON configuration that includes both SMT examples for use with the Kafka Connect REST API.
If you are running Kafka Connect in standalone mode, you can include the `transforms.*` properties in standard Java
properties syntax.

```
{
  "name": "reshaped_vehicle_events",
  "config": {
    "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
    "snowflake.ingestion.method": "SNOWPIPE_STREAMING",
    "tasks.max": "1",
    "topics": "vehicle_events",    
    "snowflake.topic2table.map": "vehicle_events:reshaped_vehicle_events",       
    "snowflake.enable.schematization": "true",        
    "buffer.count.records": "10000",
    "buffer.flush.time": "10",
    "buffer.size.bytes": "20000000",        
    "snowflake.url.name": "YOUR_ACCOUNT_IDENTIFIER.snowflakecomputing.com:443",
    "snowflake.user.name": "STREAMING_INGEST_USER",
    "snowflake.private.key": "STREAMING_INGEST_PRIVATE_KEY",
    "snowflake.database.name": "sfkafka_testing",
    "snowflake.schema.name": "raw",    
    "snowflake.role.name": "STREAMING_INGEST_ROLE",          
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",    
    "transforms": "addSinkTimestampHeader,reshapeVehicleEvent,addKafkaMetadataColumns",
    "transforms.addSinkTimestampHeader.type": "com.snowflake.examples.kafka.smt.AddSinkTimestampHeader",
    "transforms.reshapeVehicleEvent.type": "com.snowflake.examples.kafka.smt.ReshapeVehicleEvent",    
    "transforms.addKafkaMetadataColumns.type": "com.snowflake.examples.kafka.smt.AddKafkaMetadataColumns",
    "transforms.addKafkaMetadataColumns.columnNameKafkaTopic": "custom$kafka_topic",
    "transforms.addKafkaMetadataColumns.columnNameKafkaPartition": "custom$kafka_partition",
    "transforms.addKafkaMetadataColumns.columnNameKafkaOffset": "custom$kafka_offset",
  }
}
```