package com.rmondal.distributedjobscheduler.service;

import com.rmondal.distributedjobscheduler.kafka.JobProducer;
import com.rmondal.distributedjobscheduler.metrics.JobMetricsService;
import com.rmondal.distributedjobscheduler.model.Job;
import com.rmondal.distributedjobscheduler.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class StaleJobReaperService {

    private final JobRepository jobRepository;
    private final JobProducer jobProducer;
    private final JobMetricsService jobMetricsService;

    @Value("${job.reaper.timeout-ms:300000}")
    private long timeoutMs;

    @Scheduled(fixedDelayString = "${job.reaper.poll-interval-ms:60000}")
    public void reapStaleJobs() {
        LocalDateTime cutoff = LocalDateTime.now().minus(timeoutMs, ChronoUnit.MILLIS);
        List<Job> staleJobs = jobRepository.findStaleRunningJobs(cutoff);

        if (staleJobs.isEmpty()) {
            return;
        }

        log.warn("Found {} stale RUNNING job(s), recovering...", staleJobs.size());

        for (Job job : staleJobs) {
            int retried = jobRepository.retryJob(job.getId());
            if (retried > 0) {
                jobProducer.sendToRetry(job.getId());
                log.info("Stale job {} recovered and sent to retry topic (retry {})", job.getId(), job.getRetryCount() + 1);
            } else {
                jobRepository.markFailed(job.getId());
                jobProducer.sendToDlq(job.getId());
                jobMetricsService.recordFailed(job);
                jobMetricsService.recordSentToDlq(job);
                log.warn("Stale job {} marked FAILED and sent to DLQ (max retries exhausted)", job.getId());
            }
        }
    }
}
