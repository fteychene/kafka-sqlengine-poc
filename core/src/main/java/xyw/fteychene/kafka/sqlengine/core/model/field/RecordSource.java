package xyw.fteychene.kafka.sqlengine.core.model.field;

import lombok.AllArgsConstructor;
import lombok.Data;
import xyw.fteychene.kafka.sqlengine.core.model.KafkaRecord;

import java.nio.ByteBuffer;
import java.util.function.Function;

@Data
@AllArgsConstructor
public class RecordSource {

    Function<KafkaRecord, ByteBuffer> extract;

    public static RecordSource KEY = new RecordSource(KafkaRecord::getKey);
    public static RecordSource VALUE = new RecordSource(KafkaRecord::getValue);

}
