package xyw.fteychene.kafka.sqlengine.core.model.field;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class CsvSchema {

    public record CsvColumn(String name, SqlType type) {

    }

    String separator;
    List<CsvColumn> columns;
}
