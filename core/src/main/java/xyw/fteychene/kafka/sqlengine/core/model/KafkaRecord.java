package xyw.fteychene.kafka.sqlengine.core.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
public class KafkaRecord {

    String topic;
    long offset;
    int partition;
    long timestamp;
    ByteBuffer key;
    ByteBuffer value;

}
