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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import xyw.fteychene.kafka.sqlengine.core.engine.calcite.CalciteSchema;
import xyw.fteychene.kafka.sqlengine.core.engine.kafka.KafkaLoader;
import xyw.fteychene.kafka.sqlengine.core.model.KafkaTable;
import xyw.fteychene.kafka.sqlengine.core.model.field.Column;
import xyw.fteychene.kafka.sqlengine.core.model.field.JsonSchema;
import xyw.fteychene.kafka.sqlengine.core.model.field.RecordSource;
import xyw.fteychene.kafka.sqlengine.core.model.field.SqlType;

import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;

@Slf4j
public class Main {

    public static void main(String[] args) throws Exception {
        var kafkaLoader = new KafkaLoader(Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092"));
        var table = new KafkaTable("test", List.of(
                new Column.Data("key", SqlType.VARCHAR, RecordSource.KEY),
                new Column.Metadata(Column.RecordMetadata.TIMESTAMP),
                new Column.Metadata(Column.RecordMetadata.OFFSET),
                new Column.Metadata(Column.RecordMetadata.PARTITION),
                new Column.Json(Optional.empty(), new JsonSchema(List.of(
                        new JsonSchema.JsonField("id", "uuid", SqlType.VARCHAR),
                        new JsonSchema.JsonField("name", SqlType.VARCHAR),
                        new JsonSchema.JsonField("age", SqlType.INTEGER),
                        new JsonSchema.JsonField("boolean", SqlType.BOOLEAN),
                        new JsonSchema.JsonField("double", "double", SqlType.DOUBLE)
                )), RecordSource.VALUE)
        ));
        Schema calciteSchema = new CalciteSchema(List.of(table), kafkaLoader);

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

        var rsParsed = ResultSetUtil.parse(resultSet);
        log.info("Query result : \n{}", rsParsed.getValue());
        log.info("Found {} rows", rsParsed.getKey());

        resultSet.close();
        statement.close();
        connection.close();

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
