package xyw.fteychene.kafka.sqlengine.core.engine.calcite;

import lombok.AllArgsConstructor;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import xyw.fteychene.kafka.sqlengine.core.engine.kafka.KafkaLoader;
import xyw.fteychene.kafka.sqlengine.core.model.KafkaTable;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AllArgsConstructor
public class CalciteSchema extends AbstractSchema {

    List<KafkaTable> tables;
    KafkaLoader kafkaLoader;

    @Override
    protected Map<String, Table> getTableMap() {
        return tables.stream()
                .map(table -> Map.entry(table.getTopic().toUpperCase(), new CalciteTable(table, kafkaLoader)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
