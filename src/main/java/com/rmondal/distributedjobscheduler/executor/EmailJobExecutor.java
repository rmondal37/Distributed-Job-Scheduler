package com.rmondal.distributedjobscheduler.executor;

import com.rmondal.distributedjobscheduler.enums.JobType;
import com.rmondal.distributedjobscheduler.model.Job;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class EmailJobExecutor implements JobExecutor {

    @Override
    public JobType getType() {
        return JobType.EMAIL;
    }

    @Override
    public void execute(Job job) {
        log.info("Sending email for job {}: {}", job.getId(), job.getPayload());
    }
}
