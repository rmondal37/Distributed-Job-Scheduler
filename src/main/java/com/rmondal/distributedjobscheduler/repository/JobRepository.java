package com.rmondal.distributedjobscheduler.repository;

import com.rmondal.distributedjobscheduler.enums.JobStatus;
import com.rmondal.distributedjobscheduler.model.Job;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface JobRepository extends JpaRepository<Job, UUID> {

    Optional<Job> findByIdempotencyKey(String idempotencyKey);

    long countByStatus(JobStatus status);

    @Modifying
    @Transactional
    @Query("UPDATE Job j SET j.status = :newStatus, j.updatedAt = CURRENT_TIMESTAMP WHERE j.id = :id AND j.status = :expectedStatus")
    int transitionStatus(@Param("id") UUID id,
                         @Param("expectedStatus") JobStatus expectedStatus,
                         @Param("newStatus") JobStatus newStatus);

    @Modifying
    @Transactional
    @Query("UPDATE Job j SET j.status = 'PENDING', j.retryCount = j.retryCount + 1, j.updatedAt = CURRENT_TIMESTAMP " +
            "WHERE j.id = :id AND j.status = 'RUNNING' AND j.retryCount < j.maxRetries")
    int retryJob(@Param("id") UUID id);

    @Modifying
    @Transactional
    @Query("UPDATE Job j SET j.status = 'FAILED', j.updatedAt = CURRENT_TIMESTAMP " +
            "WHERE j.id = :id AND j.status = 'RUNNING'")
    int markFailed(@Param("id") UUID id);

    @Query("SELECT j FROM Job j WHERE j.status = 'RUNNING' AND j.updatedAt < :cutoff")
    List<Job> findStaleRunningJobs(@Param("cutoff") LocalDateTime cutoff);

    @Query("SELECT j FROM Job j WHERE j.status = 'PENDING' AND j.scheduledAt IS NOT NULL AND j.scheduledAt <= :now")
    List<Job> findScheduledJobsDue(@Param("now") LocalDateTime now);

    @Modifying
    @Transactional
    @Query("UPDATE Job j SET j.scheduledAt = NULL, j.updatedAt = CURRENT_TIMESTAMP " +
            "WHERE j.id = :id AND j.status = 'PENDING' AND j.scheduledAt IS NOT NULL")
    int claimScheduledJob(@Param("id") UUID id);
}
