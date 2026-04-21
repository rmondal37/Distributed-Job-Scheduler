package com.rmondal.distributedjobscheduler.service;

import com.rmondal.distributedjobscheduler.dto.JobRequest;
import com.rmondal.distributedjobscheduler.dto.JobResponse;
import com.rmondal.distributedjobscheduler.enums.JobStatus;
import com.rmondal.distributedjobscheduler.exception.JobNotFoundException;
import com.rmondal.distributedjobscheduler.kafka.JobProducer;
import com.rmondal.distributedjobscheduler.metrics.JobMetricsService;
import com.rmondal.distributedjobscheduler.model.Job;
import com.rmondal.distributedjobscheduler.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class JobService {

    private final JobRepository jobRepository;
    private final JobProducer jobProducer;
    private final JobMetricsService jobMetricsService;

    @Transactional
    public JobResponse submitJob(JobRequest request) {
        Optional<Job> existing = jobRepository.findByIdempotencyKey(request.getIdempotencyKey());
        if (existing.isPresent()) {
            log.info("Duplicate submission detected for idempotency key '{}', returning existing job {}",
                    request.getIdempotencyKey(), existing.get().getId());
            return toResponse(existing.get());
        }

        Job job = new Job();
        job.setIdempotencyKey(request.getIdempotencyKey());
        job.setType(request.getType());
        job.setStatus(JobStatus.PENDING);
        job.setPayload(request.getPayload());
        job.setPriority(request.getPriority());
        job.setRetryCount(0);
        job.setMaxRetries(request.getMaxRetries());
        job.setScheduledAt(request.getScheduledAt());

        job = jobRepository.save(job);
        jobMetricsService.recordSubmitted(job);
        log.info("Job {} saved with status PENDING", job.getId());

        if (isReadyToRun(job.getScheduledAt())) {
            jobProducer.send(job.getId(), job.getPriority());
            log.info("Job {} published for immediate processing", job.getId());
        } else {
            log.info("Job {} scheduled for {}", job.getId(), job.getScheduledAt());
        }

        return toResponse(job);
    }

    @Transactional(readOnly = true)
    public JobResponse getJob(UUID id) {
        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new JobNotFoundException("Job not found: " + id));
        return toResponse(job);
    }

    private JobResponse toResponse(Job job) {
        return JobResponse.builder()
                .id(job.getId())
                .type(job.getType())
                .status(job.getStatus())
                .payload(job.getPayload())
                .priority(job.getPriority())
                .scheduledAt(job.getScheduledAt())
                .createdAt(job.getCreatedAt())
                .updatedAt(job.getUpdatedAt())
                .build();
    }

    private boolean isReadyToRun(LocalDateTime scheduledAt) {
        return scheduledAt == null || !scheduledAt.isAfter(LocalDateTime.now());
    }
}
