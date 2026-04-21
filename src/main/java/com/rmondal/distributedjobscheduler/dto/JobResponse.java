package com.rmondal.distributedjobscheduler.dto;

import com.rmondal.distributedjobscheduler.enums.JobStatus;
import com.rmondal.distributedjobscheduler.enums.JobType;
import lombok.Builder;
import lombok.Getter;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

@Getter
@Builder
public class JobResponse {
    private UUID id;
    private JobType type;
    private JobStatus status;
    private Map<String, Object> payload;
    private int priority;
    private LocalDateTime scheduledAt;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
