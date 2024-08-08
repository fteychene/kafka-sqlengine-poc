package xyw.fteychene.kafka.sqlengine.core.model.field;

import java.util.Optional;

public sealed interface Column {

    enum RecordMetadata {
        TIMESTAMP,
        OFFSET,
        PARTITION
    }

    record Metadata(RecordMetadata source) implements Column {
    }

    record Data(String name, SqlType type, RecordSource source) implements Column {
    }

    record Json(Optional<String> prefix, JsonSchema schema, RecordSource source) implements Column {
    }

}
