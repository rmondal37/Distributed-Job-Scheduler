package com.rmondal.distributedjobscheduler.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.Map;

@Configuration
public class KafkaConfig {

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

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public NewTopic highPriorityTopic() {
        return TopicBuilder.name(highTopic).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic mediumPriorityTopic() {
        return TopicBuilder.name(mediumTopic).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic lowPriorityTopic() {
        return TopicBuilder.name(lowTopic).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic retryTopic() {
        return TopicBuilder.name(retryTopic).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic dlqTopic() {
        return TopicBuilder.name(dlqTopic).partitions(1).replicas(1).build();
    }

    @Bean(destroyMethod = "close")
    public AdminClient kafkaAdminClient() {
        return AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers
        ));
    }
}
