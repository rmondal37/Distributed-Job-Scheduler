package com.rmondal.distributedjobscheduler.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class KafkaLagMetricsService {

    private final AdminClient adminClient;
    private final MeterRegistry meterRegistry;
    private final String consumerGroupId;
    private final ConcurrentMap<String, AtomicLong> lagByTopic = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AtomicLong> recordsByTopic = new ConcurrentHashMap<>();

    public KafkaLagMetricsService(
            AdminClient adminClient,
            MeterRegistry meterRegistry,
            @Value("${spring.kafka.consumer.group-id}") String consumerGroupId) {
        this.adminClient = adminClient;
        this.meterRegistry = meterRegistry;
        this.consumerGroupId = consumerGroupId;
    }

    @Scheduled(fixedDelayString = "${management.metrics.kafka.lag.poll-interval-ms:15000}")
    public void collectLag() {
        try {
            Map<TopicPartition, OffsetAndMetadata> committedOffsets = adminClient
                    .listConsumerGroupOffsets(consumerGroupId)
                    .partitionsToOffsetAndMetadata()
                    .get();

            Map<String, Long> lagPerTopic = new HashMap<>();
            if (!committedOffsets.isEmpty()) {
                Map<TopicPartition, OffsetSpec> latestOffsetRequests = new HashMap<>();
                for (TopicPartition partition : committedOffsets.keySet()) {
                    latestOffsetRequests.put(partition, OffsetSpec.latest());
                }

                ListOffsetsResult latestOffsets = adminClient.listOffsets(latestOffsetRequests);
                for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committedOffsets.entrySet()) {
                    TopicPartition partition = entry.getKey();
                    long committed = entry.getValue().offset();
                    long latest = latestOffsets.partitionResult(partition).get().offset();
                    long lag = Math.max(latest - committed, 0L);
                    lagPerTopic.merge(partition.topic(), lag, Long::sum);
                }
            }

            lagByTopic.keySet().forEach(topic -> updateGauge(topic, lagPerTopic.getOrDefault(topic, 0L)));
            lagPerTopic.forEach(this::updateGauge);
        } catch (Exception e) {
            log.warn("Unable to collect Kafka lag metrics for group {}", consumerGroupId, e);
        }
    }

    @Scheduled(fixedDelayString = "${management.metrics.kafka.lag.poll-interval-ms:15000}")
    public void collectTopicDepth() {
        try {
            Map<String, Long> depthPerTopic = new HashMap<>();
            adminClient.listTopics().names().get().forEach(topic -> {
                try {
                    adminClient.describeTopics(java.util.List.of(topic)).allTopicNames().get().get(topic)
                            .partitions()
                            .forEach(partitionInfo -> {
                                TopicPartition topicPartition = new TopicPartition(topic, partitionInfo.partition());
                                Map<TopicPartition, OffsetSpec> earliestRequest = Map.of(topicPartition, OffsetSpec.earliest());
                                Map<TopicPartition, OffsetSpec> latestRequest = Map.of(topicPartition, OffsetSpec.latest());
                                long earliest = 0;
                                try {
                                    earliest = adminClient.listOffsets(earliestRequest).partitionResult(topicPartition).get().offset();
                                } catch (InterruptedException | ExecutionException e) {
                                    throw new RuntimeException(e);
                                }
                                long latest = 0;
                                try {
                                    latest = adminClient.listOffsets(latestRequest).partitionResult(topicPartition).get().offset();
                                } catch (InterruptedException | ExecutionException e) {
                                    throw new RuntimeException(e);
                                }
                                depthPerTopic.merge(topic, Math.max(latest - earliest, 0L), Long::sum);
                            });
                } catch (Exception e) {
                    log.warn("Unable to collect topic depth for topic {}", topic, e);
                }
            });

            recordsByTopic.keySet().forEach(topic -> updateTopicDepthGauge(topic, depthPerTopic.getOrDefault(topic, 0L)));
            depthPerTopic.forEach(this::updateTopicDepthGauge);
        } catch (Exception e) {
            log.warn("Unable to collect Kafka topic depth metrics", e);
        }
    }

    private void updateGauge(String topic, long lag) {
        lagByTopic.computeIfAbsent(topic, this::registerGauge).set(lag);
    }

    private AtomicLong registerGauge(String topic) {
        AtomicLong gaugeValue = new AtomicLong(0L);
        Gauge.builder("kafka_consumer_fetch_manager_records_lag", gaugeValue, AtomicLong::get)
                .description("Kafka consumer lag aggregated by topic")
                .tag("group", consumerGroupId)
                .tag("topic", topic)
                .register(meterRegistry);
        return gaugeValue;
    }

    private void updateTopicDepthGauge(String topic, long records) {
        recordsByTopic.computeIfAbsent(topic, this::registerTopicDepthGauge).set(records);
    }

    private AtomicLong registerTopicDepthGauge(String topic) {
        AtomicLong gaugeValue = new AtomicLong(0L);
        Gauge.builder("kafka_topic_records", gaugeValue, AtomicLong::get)
                .description("Approximate number of records currently retained in a Kafka topic")
                .tag("topic", topic)
                .register(meterRegistry);
        return gaugeValue;
    }
}
