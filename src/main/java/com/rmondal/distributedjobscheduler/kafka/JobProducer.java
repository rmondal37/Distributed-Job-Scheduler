package com.rmondal.distributedjobscheduler.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Slf4j
@Service
public class JobProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${job.kafka.topic.high}")
    private String highTopic;

    @Value("${job.kafka.topic.medium}")
    private String mediumTopic;

    @Value("${job.kafka.topic.low}")
    private String lowTopic;

    @Value("${job.kafka.topic.retry}")
    private String retryTopic;

    @Value("${job.kafka.topic.dlq}")
    private String dlqTopic;

    public JobProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(UUID jobId, int priority) {
        String topic = resolveTopic(priority);
        String key = jobId.toString();
        kafkaTemplate.send(topic, key, key);
        log.info("Job {} published to topic {}", jobId, topic);
    }

    public void sendToRetry(UUID jobId) {
        String key = jobId.toString();
        kafkaTemplate.send(retryTopic, key, key);
        log.info("Job {} published to retry topic", jobId);
    }

    public void sendToDlq(UUID jobId) {
        String key = jobId.toString();
        kafkaTemplate.send(dlqTopic, key, key);
        log.info("Job {} published to DLQ", jobId);
    }

    private String resolveTopic(int priority) {
        return switch (priority) {
            case 1 -> highTopic;
            case 2 -> mediumTopic;
            default -> lowTopic;
        };
    }
}
