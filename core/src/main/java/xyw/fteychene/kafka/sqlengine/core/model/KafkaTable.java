package xyw.fteychene.kafka.sqlengine.core.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import xyw.fteychene.kafka.sqlengine.core.model.field.Value;
import xyw.fteychene.kafka.sqlengine.core.model.field.RecordMetadata;

import java.util.List;

@Data
@AllArgsConstructor
public class KafkaTable {

    String topic;
    List<RecordMetadata> metadataColumns;
    List<Value> values;
    
}
