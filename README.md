# Lango Worker (Quarkus · Kotlin & Java 21)

Runs the ticket SLA workflow from the Lango spec (section 5): it counts down each
ticket's SLA, pauses and resumes the clock, and on a breach alerts the SRM and then
each management tier. All database writes go back to the Go backend over gRPC.

```
src/main/proto/lango_workflow.proto          shared contract (same file as the Go backend)
src/main/kotlin/.../workflows/               TicketLifecycleWorkflow + Impl  (the SLA logic)
src/main/java/.../activities/                TicketActivities + Impl         (gRPC calls to Go backend)
src/main/resources/application.properties    task queue, Temporal + gRPC settings
src/test/...                                 automated tests (see below)
```

## Requirements

- JDK 21
- Maven 3.9+ (or use the bundled `./mvnw`, which downloads Maven 3.9.11)
- For manual testing only: the [Temporal CLI](https://docs.temporal.io/cli) (or Docker)

## 1. Automated tests (start here)

```bash
./mvnw test
```

No Docker, Temporal server or Go backend needed. The tests use Temporal's in-memory
server with **time skipping**, so "wait 40 hours" takes milliseconds.

| Test | What it proves |
|---|---|
| `TicketLifecycleWorkflowTest` (Kotlin) | The SLA logic: resolved in time means no alerts; a breach alerts the SRM at exactly the deadline and then each tier after its gap; pausing moves the deadline by the paused time; repeated pauses are ignored; resolving mid-escalation stops later tiers; resolving while paused records the paused time; DECLINED/CLOSED end the workflow. |
| `TicketActivitiesImplTest` (Java) | Every field reaches the Go backend over gRPC; a `success=false` reply or a backend that's down throws a **retryable** error (so Temporal retries instead of losing the alert). |
| `WorkerWiringTest` (`@QuarkusTest`) | The whole app wired together: Quarkus registers the workflow on `lango-tickets-tq`, the real activity bean runs, and its `@GrpcClient` reaches a fake Go backend on port 19090. |

To run a single test: `./mvnw test -Dtest=TicketLifecycleWorkflowTest`

## 2. Manual testing against a real Temporal server

**a) Start Temporal with the Lango search attributes** (the workflow fails if they don't exist):

```bash
temporal server start-dev \
  --search-attribute LangoRegion=Keyword --search-attribute LangoBranch=Keyword \
  --search-attribute LangoTicketType=Keyword --search-attribute LangoTicketStatus=Keyword \
  --search-attribute LangoSlaStatus=Keyword --search-attribute LangoSlaDate=Datetime \
  --search-attribute LangoEscalationTier=Int
```

Web UI: http://localhost:8233

*(Alternative: with Docker running, `./mvnw quarkus:dev` with no target starts Temporal for you
via Dev Services. You then need to create the search attributes with
`temporal operator search-attribute create --name LangoTicketStatus --type Keyword` and so on.)*

**b) Run the worker in dev mode** (live reload on code changes):

```bash
./mvnw quarkus:dev -Dquarkus.temporal.connection.target=localhost:7233
```

**c) Act like the Go backend from a second terminal.** Payloads are protobuf, so tell the CLI
the encoding and message type:

```bash
# Raise a ticket with a 1-hour SLA and one escalation tier 1 hour later
temporal workflow start --task-queue lango-tickets-tq --type TicketLifecycleWorkflow \
  --workflow-id ticket-demo-1 \
  --input-meta encoding=json/protobuf \
  --input-meta messageType=com.tracom.lango.proto.v1.TicketWorkflowInput \
  --input '{"ticketId":"demo-1","ticketNumber":"RS10000000011772654400","ticketType":"SERVICE","srmUserId":"srm-1","slaHoursApplied":"1","escalationTierHours":["1"]}'

# Check it
temporal workflow query --workflow-id ticket-demo-1 --name getWorkflowDiagnostics

# Pause and resume the SLA clock
temporal workflow signal --workflow-id ticket-demo-1 --name pauseSla \
  --input-meta encoding=json/protobuf --input-meta messageType=com.tracom.lango.proto.v1.PauseSlaSignal \
  --input '{"actorId":"officer-1","reason":"waiting on merchant"}'
temporal workflow signal --workflow-id ticket-demo-1 --name resumeSla \
  --input-meta encoding=json/protobuf --input-meta messageType=com.tracom.lango.proto.v1.ResumeSlaSignal \
  --input '{"actorId":"officer-1"}'

# Close it (ends the workflow)
temporal workflow signal --workflow-id ticket-demo-1 --name statusTransition \
  --input-meta encoding=json/protobuf --input-meta messageType=com.tracom.lango.proto.v1.StatusTransitionSignal \
  --input '{"newStatus":"CLOSED","actorId":"officer-1"}'
```

Notes:
- The smallest SLA is 1 hour (the contract uses whole hours), so a real breach takes an hour
  to happen here. That's why breach timing is covered by the automated tests.
- If the Go backend isn't running on `localhost:9090`, the resume sync and breach alerts fail
  and **keep retrying** (visible as a pending activity in the Web UI). That's the intended
  behaviour: start the backend and they go through.

## 3. Build and run as a container

```bash
./mvnw package -DskipTests
docker build -f Dockerfile.jvm -t lango-worker .
```

Environment variables (same names as the spec's docker-compose / Helm values):

| Variable | Default | Meaning |
|---|---|---|
| `QUARKUS_TEMPORAL_CONNECTION_TARGET` | `temporal:7233` | Temporal frontend |
| `QUARKUS_TEMPORAL_NAMESPACE` | `default` | Temporal namespace |
| `GRPC_CLIENTS_LANGO_GO_BACKEND_HOST` | `localhost` | Go backend internal gRPC host |
| `GRPC_CLIENTS_LANGO_GO_BACKEND_PORT` | `9090` | Go backend internal gRPC port |

## Where this differs from the v3.2 spec, and why

1. **Annotations.** The spec uses `@WorkflowImpl(taskQueues=…)` and `@ActivityImpl(taskQueues=…)`,
   which don't exist in the quarkus-temporal extension. Here the workflow and activity
   are picked up automatically, and the queue is set with `quarkus.temporal.worker.task-queue`.
2. **No activity calls inside signal handlers.** `resumeSla` only updates state. The main
   loop then sends the paused total to the backend.
3. **Unlimited activity retries** (max 1 min apart) instead of 10 attempts. With 10, a
   backend outage of about 9 minutes would fail the workflow and stop all future
   escalations for that ticket.
4. **Pauses after a breach are ignored.** The spec allows them, but there's no clock left
   to pause. Change this in `pauseSla` if the business wants a pause to hold escalations.
5. **Diagnostics show live remaining time.** The spec only updated it when the timer woke.
6. **Env var mapping.** Quarkus wouldn't read the spec's `GRPC_CLIENTS_LANGO_GO_BACKEND_*`
   variables on its own. `application.properties` maps them.

## Not in this project yet

- `MerchantOnboardingWorkflow`. The Go recruitment handler starts it on `lango-onboarding-tq`,
  but the spec never defines it. Until a worker polls that queue, those workflows sit unstarted.
- The Go side of `RecordSlaBreachAndEscalate` (writing the breach and calling the notification
  service) lives in the backend, not here.
