package com.rmondal.distributedjobscheduler.executor;

import com.rmondal.distributedjobscheduler.enums.JobType;
import com.rmondal.distributedjobscheduler.model.Job;

public interface JobExecutor {

    JobType getType();

    void execute(Job job);
}
