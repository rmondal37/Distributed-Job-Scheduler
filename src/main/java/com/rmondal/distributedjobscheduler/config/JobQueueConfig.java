package com.rmondal.distributedjobscheduler.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Configuration
@EnableScheduling
public class JobQueueConfig {

    @Bean
    public ScheduledExecutorService retryScheduler() {
        return Executors.newSingleThreadScheduledExecutor();
    }
}
