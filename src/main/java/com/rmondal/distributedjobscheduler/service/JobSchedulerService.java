package com.rmondal.distributedjobscheduler.service;

import com.rmondal.distributedjobscheduler.kafka.JobProducer;
import com.rmondal.distributedjobscheduler.model.Job;
import com.rmondal.distributedjobscheduler.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class JobSchedulerService {

    private final JobRepository jobRepository;
    private final JobProducer jobProducer;

    @Scheduled(fixedDelayString = "${job.scheduler.poll-interval-ms:5000}")
    @Transactional
    public void pollScheduledJobs() {
        List<Job> dueJobs = jobRepository.findScheduledJobsDue(LocalDateTime.now());
        if (dueJobs.isEmpty()) {
            return;
        }

        log.info("Found {} scheduled job(s) due for execution", dueJobs.size());

        for (Job job : dueJobs) {
            int claimed = jobRepository.claimScheduledJob(job.getId());
            if (claimed == 0) {
                continue;
            }

            jobProducer.send(job.getId(), job.getPriority());
            log.info("Scheduled job {} published to topic (priority={})", job.getId(), job.getPriority());
        }
    }
}
