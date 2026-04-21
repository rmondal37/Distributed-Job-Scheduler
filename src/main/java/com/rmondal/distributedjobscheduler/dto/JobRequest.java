package com.rmondal.distributedjobscheduler.dto;

import com.rmondal.distributedjobscheduler.enums.JobType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.Map;

@Getter
@Setter
public class JobRequest {

    @NotBlank(message = "Idempotency key is required")
    private String idempotencyKey;

    @NotNull(message = "Job type is required")
    private JobType type;

    @NotNull(message = "Payload is required")
    private Map<String, Object> payload;

    private int priority = 3;

    private int maxRetries = 3;

    private LocalDateTime scheduledAt;
}
