package xyw.fteychene.kafka.sqlengine.core.engine.calcite;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import xyw.fteychene.kafka.sqlengine.core.engine.kafka.KafkaLoader;
import xyw.fteychene.kafka.sqlengine.core.model.*;
import xyw.fteychene.kafka.sqlengine.core.model.field.Column;
import xyw.fteychene.kafka.sqlengine.core.model.field.SqlType;

import java.util.Map;
import java.util.stream.Stream;

@Data
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class CalciteTable extends AbstractTable implements ScannableTable {

    private static final String FIELD_SEPARATOR = ".";
    private static final String DEFAULT_DATA_FIELD_NAME_SEPARATOR = "data";

    KafkaTable table;
    KafkaLoader kafkaLoader;

    @Override
    public Enumerable<Object[]> scan(DataContext dataContext) {
        var rowConverter = new ColumnDataTransformer(table.getColumns());
        return new AbstractEnumerable<>() {

            @Override
            public Enumerator<Object[]> enumerator() {
                return Linq4j.iterableEnumerator(kafkaLoader.fullScan(table).map(rowConverter::datas).map(Stream::toArray).toList());
            }
        };
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        var result = relDataTypeFactory.builder();
        table.getColumns().stream()
                .flatMap(field -> CalciteTable.columnsTypes(field, relDataTypeFactory))
                .forEach(entry -> result.add(entry.getKey(), entry.getValue()));
        return result.build();
    }

    public static Stream<Map.Entry<String, RelDataType>> columnsTypes(Column column, RelDataTypeFactory relDataTypeFactory) {
        return switch (column) {
            case Column.Metadata metadata -> Stream.of(switch (metadata.source()) {
                case TIMESTAMP -> Map.entry("timestamp", relDataTypeFactory.createSqlType(SqlTypeName.BIGINT));
                case OFFSET -> Map.entry("offset", relDataTypeFactory.createSqlType(SqlTypeName.BIGINT));
                case PARTITION -> Map.entry("partition", relDataTypeFactory.createSqlType(SqlTypeName.INTEGER));
            });
            case Column.Data data -> Stream.of(Map.entry(data.name(), from(data.type(), true, relDataTypeFactory)));
            case Column.Json json -> json.schema().getFields().stream()
                    .map(jsonField -> Map.entry(jsonField.name(), from(jsonField.type(), true, relDataTypeFactory)))
                    .map(jsonColumn -> Map.entry(
                            json.prefix().map(prefix -> prefix + jsonColumn.getKey()).orElse(jsonColumn.getKey()),
                            jsonColumn.getValue()));
        };
    }

    public static RelDataType from(SqlType type, boolean nullable, RelDataTypeFactory relDataTypeFactory) {
        return switch (type) {
            case VARCHAR ->
                    relDataTypeFactory.createTypeWithNullability(relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR), nullable);
            case INTEGER ->
                    relDataTypeFactory.createTypeWithNullability(relDataTypeFactory.createSqlType(SqlTypeName.INTEGER), nullable);
            case BIGINT ->
                    relDataTypeFactory.createTypeWithNullability(relDataTypeFactory.createSqlType(SqlTypeName.BIGINT), nullable);
            case BOOLEAN ->
                    relDataTypeFactory.createTypeWithNullability(relDataTypeFactory.createSqlType(SqlTypeName.BOOLEAN), nullable);
            case FLOAT ->
                    relDataTypeFactory.createTypeWithNullability(relDataTypeFactory.createSqlType(SqlTypeName.FLOAT), nullable);
            case DOUBLE ->
                    relDataTypeFactory.createTypeWithNullability(relDataTypeFactory.createSqlType(SqlTypeName.DOUBLE), nullable);
        };
    }
}
