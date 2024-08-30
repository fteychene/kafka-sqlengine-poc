package xyw.fteychene.kafka.sqlengine.core.model.field;

import java.util.Optional;

public sealed interface Value {

    record Data(String name, RecordSource source, SqlType type) implements Value {
    }

    record Json(Optional<String> prefix, RecordSource source, JsonSchema schema) implements Value {
    }

    record Csv(Optional<String> prefix, RecordSource source, CsvSchema schema) implements Value {

    }

}
