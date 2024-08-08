package xyw.fteychene.kafka.sqlengine.core.model.field;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class JsonSchema {

    public static record JsonField(String name, String jsonPath, SqlType type) {

        public JsonField(String jsonPath, SqlType type) {
            this(jsonPath, jsonPath, type);
        }

    }

    List<JsonField> fields;
}
