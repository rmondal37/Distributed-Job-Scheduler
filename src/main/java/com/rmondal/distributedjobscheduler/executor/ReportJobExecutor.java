package com.rmondal.distributedjobscheduler.executor;

import com.rmondal.distributedjobscheduler.enums.JobType;
import com.rmondal.distributedjobscheduler.model.Job;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ReportJobExecutor implements JobExecutor {

    @Override
    public JobType getType() {
        return JobType.REPORT;
    }

    @Override
    public void execute(Job job) {
        log.info("Generating report for job {}: {}", job.getId(), job.getPayload());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Report generation interrupted", e);
        }
        log.info("Report generation completed for job {}", job.getId());
    }
}
