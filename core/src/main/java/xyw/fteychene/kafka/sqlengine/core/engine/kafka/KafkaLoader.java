package xyw.fteychene.kafka.sqlengine.core.engine.kafka;

import lombok.AllArgsConstructor;
import org.apache.calcite.util.Pair;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import xyw.fteychene.kafka.sqlengine.core.model.KafkaRecord;
import xyw.fteychene.kafka.sqlengine.core.model.KafkaTable;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@AllArgsConstructor
public class KafkaLoader {

    public Map<String, Object> kafkaConsumerProperties;

    public KafkaConsumer<byte[], byte[]> kafkaConsumer(Map<String, Object> kafkaConsumerProperties) {
        var consumerProperties = new HashMap<>(kafkaConsumerProperties);
        consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "loader"+UUID.randomUUID());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkaLoader");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(consumerProperties);
    }

    public boolean finishLoading(Map<TopicPartition, Long> current, Map<TopicPartition, Long> latestOffsets) {
        return latestOffsets.entrySet().stream()
                .allMatch(lastPartitionOffset -> current.getOrDefault(lastPartitionOffset.getKey(), 0L) >= lastPartitionOffset.getValue());
    }

    public Stream<KafkaRecord> fullScan(KafkaTable table) {
        try (var kafkaConsumer = kafkaConsumer(kafkaConsumerProperties);
             var adminClient = AdminClient.create(kafkaConsumerProperties)) {

            return adminClient.describeTopics(List.of(table.getTopic())).allTopicNames()
                    .thenApply(descriptions -> descriptions.get(table.getTopic()))
                    .thenApply(description -> description.partitions().stream().map(partition -> new TopicPartition(table.getTopic(), partition.partition())).toList())
                    .toCompletionStage()
                    .thenCompose(topicPartitions -> adminClient.listOffsets(topicPartitions.stream().map(topicPartition -> Map.entry(topicPartition, OffsetSpec.latest())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))).all().toCompletionStage())
                    .thenApply(latestOffsets -> latestOffsets.entrySet().stream()
                            .map(latestOffset -> new Pair<>(latestOffset.getKey(), latestOffset.getValue().offset()))
                            .toList())
                    .thenApply(topicPartitions -> {
                        kafkaConsumer.assign(topicPartitions.stream().map(Pair::getKey).toList());
                        kafkaConsumer.seekToBeginning(topicPartitions.stream().map(Pair::getKey).toList());

                        // TODO improve loading
                        List<KafkaRecord> result = new ArrayList<>();
                        Map<TopicPartition, Long> target = topicPartitions.stream().collect(Collectors.toMap(Pair::getKey, value -> value.getValue() - 1));
                        Map<TopicPartition, Long> current = new HashMap<>();
                        while (!finishLoading(current, target)) {
                            kafkaConsumer.poll(Duration.ofMillis(500)).forEach(record ->{
                                result.add(new KafkaRecord(record.topic(), record.offset(), record.partition(), record.timestamp(),
                                        Optional.ofNullable(record.key()).map(ByteBuffer::wrap).orElse(ByteBuffer.wrap(new byte[]{})),
                                        Optional.ofNullable(record.value()).map(ByteBuffer::wrap).orElse(ByteBuffer.wrap(new byte[]{}))));
                                current.compute(new TopicPartition(record.topic(), record.partition()), (__, value) -> Optional.ofNullable(value).filter(v -> v > record.offset()).orElse(record.offset()));
                            });
                        }
                        return result.stream();
                    }).toCompletableFuture().get();
        } catch (ExecutionException | InterruptedException e) {
            // TODO err mngt
            throw new RuntimeException(e);
        }
    }

}
