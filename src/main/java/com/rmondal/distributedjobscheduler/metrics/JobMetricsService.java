package com.rmondal.distributedjobscheduler.metrics;

import com.rmondal.distributedjobscheduler.enums.JobStatus;
import com.rmondal.distributedjobscheduler.model.Job;
import com.rmondal.distributedjobscheduler.repository.JobRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class JobMetricsService {

    private final MeterRegistry meterRegistry;
    private final Timer executionTimer;

    public JobMetricsService(MeterRegistry meterRegistry, JobRepository jobRepository) {
        this.meterRegistry = meterRegistry;
        this.executionTimer = Timer.builder("jobs_execution_duration_seconds")
                .description("Execution time for job processing attempts")
                .publishPercentileHistogram()
                .register(meterRegistry);
        Gauge.builder("jobs_failed_current", jobRepository, repository -> repository.countByStatus(JobStatus.FAILED))
                .description("Current number of jobs in FAILED state")
                .register(meterRegistry);
    }

    public void recordSubmitted(Job job) {
        counter("jobs_submitted_total", job).increment();
    }

    public void recordCompleted(Job job) {
        counter("jobs_completed_total", job).increment();
    }

    public void recordFailed(Job job) {
        counter("jobs_failed_total", job).increment();
    }

    public void recordSentToDlq(Job job) {
        counter("jobs_sent_to_dlq_total", job).increment();
    }

    public void recordExecution(Duration duration) {
        executionTimer.record(duration);
    }

    private Counter counter(String name, Job job) {
        return Counter.builder(name)
                .description("Lifecycle count for distributed jobs")
                .tag("type", job.getType().name())
                .tag("priority", String.valueOf(job.getPriority()))
                .register(meterRegistry);
    }
}
