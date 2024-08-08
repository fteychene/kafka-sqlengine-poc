package xyw.fteychene.kafka.sqlengine.core.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import xyw.fteychene.kafka.sqlengine.core.model.field.Column;

import java.util.List;

@Data
@AllArgsConstructor
public class KafkaTable {

    String topic;
    List<Column> columns;
}
