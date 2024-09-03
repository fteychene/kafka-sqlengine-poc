# Calcite sql engine over Kafka

POC project to work on a light sql engine over [Apache Kafka](https://kafka.apache.org/) using [Apache Calcite](https://github.com/apache/calcite)

Todo
 - [ ] Data cache
 - [ ] Model rework
 - [ ] Optimize kafka loading
 - [ ] Index support
 - [ ] Optimize data parsing and access

### Example

Runnable example [here](core/src/main/java/xyw/fteychene/kafka/sqlengine/core/Main.java)

Define a table and value and their source
```java
var table = new KafkaTable("test",
        List.of(RecordMetadata.TIMESTAMP, RecordMetadata.OFFSET, RecordMetadata.PARTITION),
        List.of(
        new Value.Data("key", RecordSource.KEY, SqlType.VARCHAR),
        new Value.Json(Optional.empty(), RecordSource.VALUE, new JsonSchema(
        new JsonSchema.JsonField("id", "uuid", SqlType.VARCHAR),
        new JsonSchema.JsonField("name", SqlType.VARCHAR),
        new JsonSchema.JsonField("age", SqlType.INTEGER),
        new JsonSchema.JsonField("boolean", SqlType.BOOLEAN),
        new JsonSchema.JsonField("double", SqlType.DOUBLE)
        ))
        ))
));
```

Define a calcite schema with tables defined and kafka properties
```java
Schema calciteSchema = new CalciteSchema(IO_POOL, List.of(table), topic -> KafkaFullScanEnumerator.createFullScanEnumerator(kafkaAdminClient, kafkaConsumer, topic));
```

Run a query
```java
Class.forName("org.apache.calcite.jdbc.Driver");
Properties info = new Properties();
info.setProperty("lex", "JAVA");
Connection connection =
        DriverManager.getConnection("jdbc:calcite:", info);
CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
SchemaPlus rootSchema = calciteConnection.getRootSchema();
rootSchema.add("kafka", calciteSchema);
calciteConnection.setSchema("kafka");

Statement statement = calciteConnection.createStatement();
ResultSet resultSet = statement.executeQuery("select * from TEST");
```

Output on sample datas
```
╔═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║ TEST.key  TEST.timestamp  TEST.offset  TEST.partition  TEST.id                               TEST.name  TEST.age  TEST.boolean  TEST.double ║
║ 1         1723132468822   0            0               589e6dcc-bff7-42a8-952d-22315eb4a55c  Coralie    35        false         0.2404      ║
║ 2         1723132468822   1            0               54d125f6-be81-4878-808d-94d49f9dd336  Arnoldo    35        true          0.3722      ║
║ 3         1723132468822   2            0               8d089a39-7ab8-46c2-a8d2-ba2ac5cf4b00  Sabina     35        true          0.7739      ║
║ 4         1723132468822   3            0               5aa73825-f248-4127-95cf-9df208fb58c6  Dean       35        true          0.6072      ║
║ 5         1723132468822   4            0               2122b81a-78f3-42cf-9533-c91a98ca9da0  Eula       35        true          0.9807      ║
║ 6         1723132468822   5            0               1c84598c-b093-4f4e-a3f0-25f6993fbe07  Frances    35        false         0.7621      ║
║ 7         1723132468822   6            0               11da3626-5a60-4ed7-9d39-39e07f88db19  Natalie    35        true          0.3559      ║
╚═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝
```

## Benchmark 

Setup
 - Local Kafka
 - Topic with 1 partition
 - Dataset 