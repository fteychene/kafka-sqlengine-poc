package xyw.fteychene.kafka.sqlengine.core.engine.calcite;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import xyw.fteychene.kafka.sqlengine.core.model.KafkaRecord;
import xyw.fteychene.kafka.sqlengine.core.model.field.Column;
import xyw.fteychene.kafka.sqlengine.core.model.field.JsonSchema;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Stream;

@Data
public class ColumnDataTransformer {

    List<Column> columns;
    ObjectMapper objectMapper = new ObjectMapper();

    public ColumnDataTransformer(List<Column> columns) {
        this.columns = columns;
    }

    public Stream<Object> datas(KafkaRecord record) {
        return columns.stream()
                .flatMap(column -> switch (column) {
                    case Column.Data data -> data.source().getExtract().andThen(value -> switch (data.type()) {
                        case VARCHAR -> new String(value.array());
                        case INTEGER -> value.getInt();
                        case BIGINT -> value.getLong();
                        case BOOLEAN -> value.get() == 1;
                        case FLOAT -> value.getFloat();
                        case DOUBLE -> value.getDouble();
                    }).andThen(Stream::of).apply(record);
                    case Column.Json json ->
                            json.source().getExtract().andThen(value -> jsonColumn(value, json.schema())).apply(record);
                    case Column.Metadata metadata -> Stream.of(metadataColumn(metadata.source(), record));
                });
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

    Object metadataColumn(Column.RecordMetadata metadataSource, KafkaRecord record) {
        return switch (metadataSource) {
            case TIMESTAMP -> record.getTimestamp();
            case PARTITION -> record.getPartition();
            case OFFSET -> record.getOffset();
        };
    }
}
