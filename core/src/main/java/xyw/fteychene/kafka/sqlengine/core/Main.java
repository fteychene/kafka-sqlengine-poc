package xyw.fteychene.kafka.sqlengine.core;

import com.acidmanic.consoletools.drawing.AsciiBorders;
import com.acidmanic.consoletools.drawing.Padding;
import com.acidmanic.consoletools.table.Cell;
import com.acidmanic.consoletools.table.Row;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.Pair;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import xyw.fteychene.kafka.sqlengine.core.engine.calcite.CalciteSchema;
import xyw.fteychene.kafka.sqlengine.core.engine.calcite.KafkaFullScanEnumerator;
import xyw.fteychene.kafka.sqlengine.core.model.KafkaTable;
import xyw.fteychene.kafka.sqlengine.core.model.field.*;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

@Slf4j
public class Main {

    public static ExecutorService IO_POOL = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() / 2, Thread.ofVirtual().factory());

    public static void main(String[] args) throws Exception {
        Map<String, Object> kafkaProperties = Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");

        var kafkaAdminClient = AdminClient.create(kafkaProperties);
        Map<String, Object> consumerProperties = new HashMap<>(kafkaProperties);
        consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "loader"+UUID.randomUUID());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkaLoader"+UUID.randomUUID());
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100000);
        var kafkaConsumer = new KafkaConsumer<byte[], byte[]>(consumerProperties);

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
                ));
        var custom = new KafkaTable("custom",
                List.of(RecordMetadata.TIMESTAMP, RecordMetadata.OFFSET, RecordMetadata.PARTITION),
                List.of(new Value.Csv(Optional.empty(), RecordSource.VALUE, new CsvSchema(",", List.of(
                        new CsvSchema.CsvColumn("yearm", SqlType.INTEGER),
                        new CsvSchema.CsvColumn("exp_imp", SqlType.INTEGER),
                        new CsvSchema.CsvColumn("hs9", SqlType.VARCHAR),
                        new CsvSchema.CsvColumn("Customs", SqlType.VARCHAR),
                        new CsvSchema.CsvColumn("Country", SqlType.VARCHAR),
                        new CsvSchema.CsvColumn("Q1", SqlType.VARCHAR),
                        new CsvSchema.CsvColumn("Q2", SqlType.INTEGER),
                        new CsvSchema.CsvColumn("val", SqlType.DOUBLE)
                )))));

        Schema calciteSchema = new CalciteSchema(IO_POOL, List.of(table, custom), topic -> KafkaFullScanEnumerator.createFullScanEnumerator(kafkaAdminClient, kafkaConsumer, topic));

        var facialStart = Instant.now();

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
        ResultSet resultSet = statement.executeQuery("select yearm, SUM(val) as amount from CUSTOM group by yearm order by yearm");

        var rsParsed = ResultSetUtil.parse(resultSet);
        log.info("Query result : \n{}", rsParsed.getValue());
        log.info("Found {} rows", rsParsed.getKey());
        log.info("Ran query in {} ms (facial time)", Duration.between(facialStart, Instant.now()).toMillis());

        resultSet.close();
        statement.close();
        connection.close();

        kafkaConsumer.close();
        kafkaAdminClient.close();
    }


    @Slf4j
    public static class ResultSetUtil {

        public static Pair<Integer, String> parse(ResultSet resultSet) throws SQLException {
            com.acidmanic.consoletools.table.Table table = new com.acidmanic.consoletools.table.Table();
            table.setBorder(AsciiBorders.DOUBLELINE);

            Row columnsRow = new Row();
            IntStream.range(1, resultSet.getMetaData().getColumnCount() + 1).boxed()
                    .map(index -> {
                        try {
                            return Optional.ofNullable(resultSet.getMetaData().getTableName(index)).map(x -> x + ".").orElse("") + resultSet.getMetaData().getColumnName(index);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .forEach(column -> columnsRow.getCells().add(new Cell(column)));
            table.getRows().add(columnsRow);

            var counter = 0;
            while (resultSet.next()) {
                counter++;
                var row = new Row();
                IntStream.range(1, resultSet.getMetaData().getColumnCount() + 1).boxed()
                        .map(index -> {
                            try {
                                return resultSet.getString(index);
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .forEach(value -> row.getCells().add(new Cell(value)));
                table.getRows().add(row);
            }

            table.setCellsPadding(new Padding(1, 0));
            return new Pair<>(counter, table.render());
        }

    }
}
