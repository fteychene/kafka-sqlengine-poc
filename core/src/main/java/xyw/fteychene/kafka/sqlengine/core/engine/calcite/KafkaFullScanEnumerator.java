package xyw.fteychene.kafka.sqlengine.core.engine.calcite;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import xyw.fteychene.kafka.sqlengine.core.model.KafkaRecord;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
public class KafkaFullScanEnumerator implements Enumerator<KafkaRecord> {

    @Value
    @Jacksonized
    @AllArgsConstructor
    @Builder(toBuilder = true)
    public static class SourceLocation {
        int partition;
        Long offset;
    }

    public static final Duration CONSUMER_POLL_DURATION = Duration.ofMillis(500);
    public static final int BUFFER_TARGET_SIZE = 300000;

    final String topic;
    final List<SourceLocation> limitSourceLocation;
    final KafkaConsumer<byte[], byte[]> kafkaConsumer;
    final Deque<KafkaRecord> bufferedEvents = new ConcurrentLinkedDeque<>();
    final Map<Integer, Boolean> partitionLoadCompleted;
    final Map<Integer, Boolean> partitionServeCompleted;
    final AtomicBoolean closed = new AtomicBoolean(false);
    final AtomicReference<KafkaRecord> current = new AtomicReference<>();
    final boolean externallyManagedConsumer;

    // TODO exposed as metric
    final AtomicInteger loadCounter = new AtomicInteger(0);
    final AtomicInteger serveCounter = new AtomicInteger(0);

    public static KafkaFullScanEnumerator createFullScanEnumerator(Map<String, Object> kafkaProperties, String topic) {
        Map<String, Object> consumerProperties = new HashMap<>(kafkaProperties);
        consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "loader" + UUID.randomUUID());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkaLoader" + UUID.randomUUID());
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        try (var admin = KafkaAdminClient.create(kafkaProperties)) {
            return admin.describeTopics(List.of(topic)).allTopicNames()
                    .thenApply(descriptions -> descriptions.get(topic))
                    .thenApply(description -> description.partitions().stream().map(partition -> new TopicPartition(topic, partition.partition())).toList())
                    .toCompletionStage()
                    .thenCompose(topicPartitions -> admin.listOffsets(topicPartitions.stream().map(topicPartition -> Map.entry(topicPartition, OffsetSpec.latest())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))).all().toCompletionStage())
                    .thenApply(latestOffsets -> latestOffsets.entrySet().stream()
                            .map(latestOffset -> new SourceLocation(latestOffset.getKey().partition(), latestOffset.getValue().offset()))
                            .toList())
                    .thenApply(limitSourceLocation -> new KafkaFullScanEnumerator(topic, limitSourceLocation, new KafkaConsumer<>(consumerProperties), false))
                    .toCompletableFuture()
                    .get();
        } catch (ExecutionException | InterruptedException e) {
            // TODO error management
            throw new RuntimeException(e);
        }
    }

    public static KafkaFullScanEnumerator createFullScanEnumerator(AdminClient adminClient, KafkaConsumer<byte[], byte[]> consumer, String topic) {
        try {
            return adminClient.describeTopics(List.of(topic)).allTopicNames()
                    .thenApply(descriptions -> descriptions.get(topic))
                    .thenApply(description -> description.partitions().stream().map(partition -> new TopicPartition(topic, partition.partition())).toList())
                    .toCompletionStage()
                    .thenCompose(topicPartitions -> adminClient.listOffsets(topicPartitions.stream().map(topicPartition -> Map.entry(topicPartition, OffsetSpec.latest())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))).all().toCompletionStage())
                    .thenApply(latestOffsets -> latestOffsets.entrySet().stream()
                            .map(latestOffset -> new SourceLocation(latestOffset.getKey().partition(), latestOffset.getValue().offset()))
                            .toList())
                    .thenApply(limitSourceLocation -> new KafkaFullScanEnumerator(topic, limitSourceLocation, consumer, true))
                    .toCompletableFuture()
                    .get();
        } catch (ExecutionException | InterruptedException e) {
            // TODO error management
            throw new RuntimeException(e);
        }
    }

    public KafkaFullScanEnumerator(String topic, List<SourceLocation> limitSourceLocation, KafkaConsumer<byte[], byte[]> kafkaConsumer, boolean externallyManagedConsumer) {
        this.topic = topic;
        this.limitSourceLocation = limitSourceLocation;
        this.externallyManagedConsumer = externallyManagedConsumer;
        this.kafkaConsumer = kafkaConsumer;
        this.partitionLoadCompleted = limitSourceLocation.stream().map(sourceLocation -> Map.entry(sourceLocation.getPartition(), false))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        this.partitionServeCompleted = limitSourceLocation.stream().map(sourceLocation -> Map.entry(sourceLocation.getPartition(), false))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        kafkaConsumer.assign(limitSourceLocation.stream().map(source -> new TopicPartition(topic, source.getPartition())).toList());
        kafkaConsumer.seekToBeginning(limitSourceLocation.stream().map(sourceLocation -> new TopicPartition(topic, sourceLocation.getPartition())).toList());
    }

    @Override
    public KafkaRecord current() {
        return current.get();
    }

    public void pollUntilFinished() {
        Instant startTime = Instant.now();
        while (!closed.get() && !partitionServeCompleted.values().stream().reduce(true, (x, y) -> x && y)) {
            if (bufferedEvents.size() <= BUFFER_TARGET_SIZE) {
                bufferedEvents.addAll(poll());
            }
        }
        log.info("Loaded {}[{} records] in {}ms", topic, loadCounter.get(), Duration.between(startTime, Instant.now()).toMillis());
    }

    @Override
    public boolean moveNext() {
        if (partitionServeCompleted.values().stream().reduce(true, (x, y) -> x && y)) {
            closed.set(true);
            return false;
        }
        while (bufferedEvents.isEmpty()) {
//            log.warn("Waiting for background loader");
        }
        var now = bufferedEvents.removeFirst();
        current.set(now);
        limitSourceLocation.stream()
                .filter(location -> location.getPartition() == now.getPartition())
                .filter(location -> location.getOffset() - 1 <= now.getOffset())
                .findAny()
                .ifPresent(scannedLocation -> partitionServeCompleted.put(scannedLocation.getPartition(), true));
        serveCounter.incrementAndGet();
        return true;
    }

    public List<KafkaRecord> poll() {
        if (partitionLoadCompleted.values().stream().reduce(true, (x, y) -> x && y)) {
            return List.of();
        }
        List<KafkaRecord> result = new ArrayList<>();
        kafkaConsumer.poll(CONSUMER_POLL_DURATION)
                .forEach(consumerRecord -> {
                    result.add(new KafkaRecord(consumerRecord.topic(), consumerRecord.offset(), consumerRecord.partition(), consumerRecord.timestamp(),
                            Optional.ofNullable(consumerRecord.key()).map(ByteBuffer::wrap).orElse(ByteBuffer.wrap(new byte[]{})),
                            Optional.ofNullable(consumerRecord.value()).map(ByteBuffer::wrap).orElse(ByteBuffer.wrap(new byte[]{}))));
                    loadCounter.getAndIncrement();
                    limitSourceLocation.stream()
                            .filter(location -> location.getPartition() == consumerRecord.partition())
                            .filter(location -> location.getOffset() - 1 <= consumerRecord.offset())
                            .findAny()
                            .ifPresent(scannedLocation -> partitionLoadCompleted.put(scannedLocation.getPartition(), true));
                });
        return result;
    }

    @Override
    public void reset() {
        bufferedEvents.clear();
        kafkaConsumer.seekToBeginning(limitSourceLocation.stream().map(sourceLocation -> new TopicPartition(topic, sourceLocation.getPartition())).toList());
    }

    @Override
    public void close() {
        closed.set(true);
        log.info("Closing kafka full scan enumerator for {} with {} event loaded and {} event served.", topic, loadCounter.get(), serveCounter.get());
        if (!externallyManagedConsumer) {
            kafkaConsumer.close();
        }
    }
}
