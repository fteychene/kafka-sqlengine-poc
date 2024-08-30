package xyw.fteychene.kafka.sqlengine.core.engine.calcite;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import xyw.fteychene.kafka.sqlengine.core.model.KafkaRecord;
import xyw.fteychene.kafka.sqlengine.core.model.field.Value;
import xyw.fteychene.kafka.sqlengine.core.model.field.CsvSchema;
import xyw.fteychene.kafka.sqlengine.core.model.field.JsonSchema;
import xyw.fteychene.kafka.sqlengine.core.model.field.RecordMetadata;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Data
@AllArgsConstructor
public class ColumnDataTransformer {

    List<RecordMetadata> metadataColumns;
    List<Value> values;
    ObjectMapper objectMapper = new ObjectMapper();

    public ColumnDataTransformer(List<RecordMetadata> metadataColumns, List<Value> values) {
        this.metadataColumns = metadataColumns;
        this.values = values;
    }

    public Stream<Object> datas(KafkaRecord record) {
        return Stream.concat(
                metadataColumns.stream()
                        .map(metadata -> metadataColumn(metadata, record)),
                values.stream()
                        .flatMap(column -> switch (column) {
                            case Value.Data data -> data.source().getExtract().andThen(value -> switch (data.type()) {
                                case VARCHAR -> new String(value.array());
                                case INTEGER -> value.getInt();
                                case BIGINT -> value.getLong();
                                case BOOLEAN -> value.get() == 1;
                                case FLOAT -> value.getFloat();
                                case DOUBLE -> value.getDouble();
                            }).andThen(Stream::of).apply(record);
                            case Value.Json json ->
                                    json.source().getExtract().andThen(value -> jsonColumn(value, json.schema())).apply(record);
                            case Value.Csv csv ->
                                    csv.source().getExtract().andThen(value -> csvColumn(value, csv.schema())).apply(record);
                        }));
    }

    @SneakyThrows
    Stream<Object> jsonColumn(ByteBuffer source, JsonSchema schema) {
        var jsonTree = objectMapper.readTree(source.array());
        return schema.getFields().stream()
                .map(field -> {
                    var value = jsonTree.get(field.jsonPath());
                    return switch (field.type()) {
                        case VARCHAR -> value.asText();
                        case INTEGER -> value.asInt();
                        case BIGINT -> value.asLong();
                        case BOOLEAN -> value.asBoolean();
                        case FLOAT, DOUBLE -> value.asDouble();
                    };
                });
    }

    @SneakyThrows
    Stream<Object> csvColumn(ByteBuffer source, CsvSchema schema) {
        String row = new String(source.array());
        String[] values = row.split(schema.getSeparator());
        return IntStream.range(0, schema.getColumns().size())
                .boxed()
                .map(index -> switch (schema.getColumns().get(index).type()) {
                    case VARCHAR -> values[index];
                    case INTEGER -> Integer.valueOf(values[index]);
                    case BIGINT -> Long.valueOf(values[index]);
                    case BOOLEAN -> Boolean.parseBoolean(values[index]);
                    case FLOAT, DOUBLE -> Double.valueOf(values[index]);
                });
    }

    Object metadataColumn(RecordMetadata metadataSource, KafkaRecord record) {
        return switch (metadataSource) {
            case TIMESTAMP -> record.getTimestamp();
            case PARTITION -> record.getPartition();
            case OFFSET -> record.getOffset();
        };
    }
}
