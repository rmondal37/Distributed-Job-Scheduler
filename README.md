# Distributed Job Scheduler

`distributed-job-scheduler` is a Spring Boot based backend system for asynchronous job processing. It accepts jobs over HTTP, persists them in PostgreSQL, routes them to Kafka topics based on priority, and executes them through background consumers with retry, delayed scheduling, stale-job recovery, and DLQ handling.

The project is intended as a learning-friendly but production-inspired scheduler that demonstrates reliable background processing patterns and operational visibility with Prometheus and Grafana.

## Tech Stack

- Java 17
- Spring Boot
- Spring Data JPA
- PostgreSQL
- Kafka
- Flyway
- Micrometer + Prometheus
- Grafana
- Docker Compose

## Architecture Overview

The system is organized into a few clear responsibilities:

- `JobController` exposes APIs for job submission and job lookup.
- `JobService` validates and persists incoming jobs, enforces idempotency, and publishes ready jobs to Kafka.
- `JobProducer` sends job IDs to priority-based Kafka topics: `jobs.high`, `jobs.medium`, `jobs.low`, plus `jobs.retry` and `jobs.dlq`.
- `JobConsumer` listens to Kafka, claims jobs safely from the database, executes them using the correct executor, and handles retries or DLQ routing on failure.
- `JobSchedulerService` periodically scans for delayed jobs whose `scheduledAt` time has arrived and publishes them for execution.
- `StaleJobReaperService` detects stuck `RUNNING` jobs and either retries or moves them to DLQ after retry exhaustion.
- `JobRepository` provides atomic status transitions so multiple workers can coordinate safely through the database.
- `JobMetricsService` and `KafkaLagMetricsService` expose operational metrics for throughput, latency, failures, lag, and DLQ health.

## Request / Execution Flow

1. A client submits a job using `POST /jobs`.
2. The job is stored in PostgreSQL with status `PENDING`.
3. If the job is ready to run immediately, its ID is published to the Kafka topic that matches its priority.
4. A Kafka consumer receives the job ID and atomically transitions the job from `PENDING` to `RUNNING`.
5. The correct executor processes the job based on `JobType`.
6. On success, the job is marked `COMPLETED`.
7. On failure, the job is retried with exponential backoff until `maxRetries` is exhausted.
8. If retries are exhausted, the job is marked `FAILED` and its ID is published to the dead-letter topic.

## Current Features

- Asynchronous background job execution
- Priority-based routing with separate Kafka topics
- Idempotent job submission
- Delayed / scheduled jobs
- Retry with exponential backoff and jitter
- Dead-letter queue routing
- Stale job recovery
- Prometheus metrics and Grafana dashboards
- Postman collection for local API testing

## Project Structure

```text
src/main/java/com/rmondal/distributedjobscheduler
├── config        # Kafka, scheduler, and application configuration
├── controller    # REST endpoints
├── dto           # Request / response models
├── enums         # Job type and status enums
├── exception     # API error handling
├── executor      # Job execution implementations
├── kafka         # Producer and consumer components
├── metrics       # Prometheus / Micrometer instrumentation
├── model         # JPA entity model
├── repository    # Database access and atomic state transitions
└── service       # Scheduling, submission, retry, and recovery logic
```

## Local Setup

### Prerequisites

- Java 17
- Docker and Docker Compose

### 1. Start infrastructure

This starts PostgreSQL, Kafka, Prometheus, and Grafana:

```bash
docker compose up -d
```

Useful checks:

```bash
docker compose ps
docker compose logs kafka
docker compose logs postgres
```

### 2. Run the Spring Boot application

```bash
./gradlew bootRun
```

If dependencies are not already cached, Gradle may need internet access the first time.

### 3. Verify the app is running

```bash
curl http://localhost:8080/actuator/health
curl http://localhost:8080/actuator/prometheus | head
```

## API Endpoints

### Submit a job

```bash
curl -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "idempotencyKey": "job-1",
    "type": "EMAIL",
    "payload": {
      "to": "alice@example.com",
      "subject": "Welcome"
    },
    "priority": 1,
    "maxRetries": 3
  }'
```

### Get job by ID

```bash
curl http://localhost:8080/jobs/<JOB_ID>
```

### Submit a delayed job

```bash
curl -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "idempotencyKey": "job-2",
    "type": "REPORT",
    "payload": {
      "reportName": "daily-summary"
    },
    "priority": 2,
    "maxRetries": 3,
    "scheduledAt": "2026-04-20T10:00:00"
  }'
```

## Postman Collection

Ready-to-import Postman files are available in:

- [postman/distributed-job-scheduler.postman_collection.json](/Users/rishavmondal/Documents/distributed-job-scheduler/postman/distributed-job-scheduler.postman_collection.json)
- [postman/distributed-job-scheduler.local.postman_environment.json](/Users/rishavmondal/Documents/distributed-job-scheduler/postman/distributed-job-scheduler.local.postman_environment.json)

The collection includes:

- Health check
- Job submission
- Get job by ID
- Delayed job submission
- Duplicate idempotency key flow

## Observability

The app exposes Prometheus metrics on:

- `http://localhost:8080/actuator/prometheus`

Prometheus is available at:

- `http://localhost:9091`

Grafana is available at:

- `http://localhost:3000`

Default Grafana credentials:

- Username: `admin`
- Password: `admin`

The dashboard tracks:

- Job throughput
- Failure rate
- P99 execution latency
- Jobs completed by type
- Kafka lag by topic
- High-priority topic lag
- DLQ arrival rate
- DLQ topic depth
- Current failed jobs in the database

## Notes About DLQ Behavior

When a job exhausts retries, the application marks it `FAILED` and publishes its ID to the `jobs.dlq` topic. There is currently no dedicated DLQ consumer or replay workflow in the repository, so failed jobs remain in the database and the DLQ topic until Kafka retention removes the message or a future recovery flow is added.

## Possible Next Steps / Future Scope

- Add a dedicated worker service so API and worker responsibilities can scale independently.
- Support multiple application instances and document production deployment patterns.
- Add authentication, authorization, and tenant isolation for multi-user workloads.
- Persist richer failure metadata such as stack traces, last error message, and retry history.
- Build a DLQ replay / admin UI for failed job inspection and reprocessing.
- Add alerting rules for failure spikes, rising Kafka lag, and DLQ growth.
- Improve test coverage with integration tests for Kafka, retries, scheduling, and recovery paths.
- Introduce rate limiting, quotas, and backpressure controls.
- Add more job types and pluggable executor registration.
- Externalize configuration for cloud deployment environments.

## Why It Is Called "Distributed"

The project uses distributed systems building blocks: Kafka-based asynchronous dispatch, database-backed coordination, and worker-safe state transitions. In local development it runs as a single application instance, but the design is intended to support multiple worker instances consuming from shared Kafka topics and coordinating through PostgreSQL.

