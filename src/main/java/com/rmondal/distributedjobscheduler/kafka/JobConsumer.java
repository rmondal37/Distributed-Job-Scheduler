package com.rmondal.distributedjobscheduler.kafka;

import com.rmondal.distributedjobscheduler.enums.JobStatus;
import com.rmondal.distributedjobscheduler.enums.JobType;
import com.rmondal.distributedjobscheduler.executor.JobExecutor;
import com.rmondal.distributedjobscheduler.metrics.JobMetricsService;
import com.rmondal.distributedjobscheduler.model.Job;
import com.rmondal.distributedjobscheduler.repository.JobRepository;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class JobConsumer {

    private final JobRepository jobRepository;
    private final Map<JobType, JobExecutor> executorMap;
    private final JobProducer jobProducer;
    private final ScheduledExecutorService retryScheduler;
    private final JobMetricsService jobMetricsService;

    @Value("${job.retry.base-delay-ms:1000}")
    private long baseDelayMs;

    @Value("${job.retry.max-delay-ms:60000}")
    private long maxDelayMs;

    public JobConsumer(
            JobRepository jobRepository,
            List<JobExecutor> executors,
            JobProducer jobProducer,
            ScheduledExecutorService retryScheduler,
            JobMetricsService jobMetricsService) {
        this.jobRepository = jobRepository;
        this.jobProducer = jobProducer;
        this.retryScheduler = retryScheduler;
        this.jobMetricsService = jobMetricsService;
        this.executorMap = new HashMap<>();
        for (JobExecutor executor : executors) {
            this.executorMap.put(executor.getType(), executor);
        }
    }

    @KafkaListener(
            topics = {
                    "${job.kafka.topic.high}",
                    "${job.kafka.topic.medium}",
                    "${job.kafka.topic.low}",
                    "${job.kafka.topic.retry}"
            },
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(String jobIdStr) {
        try {
            UUID jobId = UUID.fromString(jobIdStr);
            processJob(jobId);
        } catch (Exception e) {
            log.error("Failed to process message: {}", jobIdStr, e);
        }
    }

    private void processJob(UUID jobId) {
        Job job = null;
        long executionStartedAt = 0L;
        try {
            int claimed = jobRepository.transitionStatus(jobId, JobStatus.PENDING, JobStatus.RUNNING);
            if (claimed == 0) {
                log.warn("Job {} could not be claimed (not PENDING or already taken), skipping", jobId);
                return;
            }
            log.info("Job {} marked as RUNNING", jobId);

            job = jobRepository.findById(jobId).orElseThrow(
                    () -> new IllegalStateException("Job " + jobId + " disappeared after claim"));

            JobExecutor executor = executorMap.get(job.getType());
            if (executor == null) {
                throw new IllegalStateException("No executor found for job type: " + job.getType());
            }

            executionStartedAt = System.nanoTime();
            executor.execute(job);
            jobMetricsService.recordExecution(Duration.ofNanos(System.nanoTime() - executionStartedAt));

            job.setStatus(JobStatus.COMPLETED);
            jobRepository.save(job);
            jobMetricsService.recordCompleted(job);
            log.info("Job {} marked as COMPLETED", jobId);

        } catch (Exception e) {
            if (executionStartedAt > 0L) {
                jobMetricsService.recordExecution(Duration.ofNanos(System.nanoTime() - executionStartedAt));
            }
            log.error("Job {} failed: {}", jobId, e.getMessage(), e);
            handleFailure(jobId, job);
        }
    }

    private void handleFailure(UUID jobId, Job job) {
        try {
            int retried = jobRepository.retryJob(jobId);
            if (retried > 0) {
                Job retriedJob = jobRepository.findById(jobId).orElse(null);
                int retryCount = (retriedJob != null) ? retriedJob.getRetryCount() : 1;
                long delay = calculateBackoff(retryCount);
                log.info("Job {} scheduling retry {}{} after {}ms",
                        jobId, retryCount,
                        (retriedJob != null) ? "/" + retriedJob.getMaxRetries() : "", delay);

                retryScheduler.schedule(
                        () -> jobProducer.sendToRetry(jobId), delay, TimeUnit.MILLISECONDS);
            } else {
                jobRepository.markFailed(jobId);
                jobProducer.sendToDlq(jobId);
                if (job != null) {
                    jobMetricsService.recordFailed(job);
                    jobMetricsService.recordSentToDlq(job);
                }
                log.warn("Job {} marked as FAILED and sent to DLQ", jobId);
            }
        } catch (Exception ex) {
            log.error("Failed to handle failure for job {}: {}", jobId, ex.getMessage());
        }
    }

    private long calculateBackoff(int retryCount) {
        long delay = (long) (baseDelayMs * Math.pow(2, retryCount - 1));
        delay = Math.min(delay, maxDelayMs);
        long jitter = ThreadLocalRandom.current().nextLong(0, delay / 2 + 1);
        return delay + jitter;
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down retry scheduler...");
        retryScheduler.shutdown();
        try {
            if (!retryScheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                retryScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            retryScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
