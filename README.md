# Lango Worker (Quarkus · Kotlin & Java 21)

Runs three workflows from the Lango v3.2 spec. All database writes go back to the Go
backend over gRPC.

- **`TicketLifecycleWorkflow`** on `lango-tickets-tq` (spec section 5): counts down each
  ticket's SLA, pauses and resumes the clock, and on a breach alerts the SRM and then each
  management tier.
- **`MerchantOnboardingWorkflow`** on `lango-onboarding-tq`: follows a recruitment
  application from `AWAITING_REVIEW` to `ACTIVATED` or `DECLINED` (see
  [Merchant onboarding](#merchant-onboarding)).
- **`VisitCycleWorkflow`** on `lango-visits-tq`: keeps one merchant outlet's recurring site
  visits going for as long as the cycle is active (see [Site visit cycles](#site-visit-cycles)).

```
src/main/proto/lango_workflow.proto          shared contract (same file as the Go backend)
src/main/kotlin/.../workflows/               TicketLifecycleWorkflow, MerchantOnboardingWorkflow, VisitCycleWorkflow + Impls
src/main/java/.../activities/                Ticket-, Onboarding-, VisitActivities + Impls (gRPC calls to Go backend)
src/main/resources/application.properties    task queues, onboarding + visit policy, Temporal + gRPC settings
src/test/...                                 automated tests (see below)
```

## Merchant onboarding

The spec defines how Go starts this workflow (section 4.3), the stages
(`merchants.onboarding_applications.current_stage`), the role that owns each stage, and
that site inspections and amendments are linked tickets (`merchants.application_tickets`).
It doesn't define the signals, deadlines or callbacks, so those are designed here and
**the Go backend has to implement its side** (see the bottom of this README).

```
AWAITING_REVIEW ──APPROVE──▶ COMPLIANCE_REVIEW ──APPROVE──▶ ICT_PROVISIONING ──APPROVE──▶ ProvisionMerchant ─▶ ACTIVATED
 (BRANCH_SUPERVISOR)   │       (COMPLIANCE_OFFICER)    │       (ICT_OFFICER)       │          (retried until it succeeds)
        ▲         RETURN                          RETURN                    DECLINE (any stage) ──▶ DECLINED
        │              ▼                               ▼
        └─resubmit── RETURNED_FOR_AMENDMENT (SALES_AGENT) ──no resubmit in time / too many returns──▶ DECLINED
```

**Who does what.** The backend stays the state machine enforcer: it checks the reviewer's
role, stores the new stage, then signals the workflow. The workflow owns what needs time
or retries:

- **Stage deadlines.** If a review stage overruns, the stage owners are alerted (tier 0),
  then management tiers after each gap. The clock **pauses while a linked ticket is open**
  (site inspection, clarification), because those tickets have their own SLA.
- **Amendments.** The sales agent gets a window to resubmit; after that, or after one
  return too many, the workflow declines the application.
- **Provisioning.** On ICT approval it asks the backend to provision in TMS / Core Banking,
  retried until it succeeds.

| Contract | Name | Payload |
|---|---|---|
| Start (from `RecruitMerchant`) | `MerchantOnboardingWorkflow`, id `onboarding-<applicationId>` | **plain JSON map**, not proto: `applicationId`, `ticketId`, `ticketNumber`, `recruitingAgentId`, `branchCode`, `regionCode` |
| Signal | `stageDecision` | `OnboardingStageDecisionSignal`: `stage` it was made in, `outcome` = `APPROVE` / `RETURN` / `DECLINE`. A decision for a stage the application is no longer in is ignored, so double clicks are harmless. |
| Signal | `amendmentResubmitted` | `AmendmentResubmittedSignal` |
| Signal | `linkedTicketOpened` / `linkedTicketResolved` | `LinkedTicketSignal` |
| Query | `getOnboardingDiagnostics` | `OnboardingWorkflowDiagnostics` |
| Callback | `OnboardingCallbackService.NotifyOnboardingStageOverdue` | stage overran; tier 0 = stage owners |
| Callback | `OnboardingCallbackService.DeclineOnboardingApplication` | only for `AMENDMENT_EXPIRED` / `MAX_AMENDMENTS_EXCEEDED` |
| Callback | `OnboardingCallbackService.ProvisionMerchant` | backend provisions, creates merchant + outlet, sets `ACTIVATED` |

**Policy** (`lango.onboarding.*` in `application.properties`, each with an env var). These
are **placeholders until the business confirms them**. Each workflow reads them once when
it starts, so a change only affects new applications.

| Setting | Env var | Default |
|---|---|---|
| Review deadline (hours) | `LANGO_ONBOARDING_REVIEW_DEADLINE_HOURS` | 24 |
| Compliance deadline (hours) | `LANGO_ONBOARDING_COMPLIANCE_DEADLINE_HOURS` | 48 |
| ICT deadline (hours) | `LANGO_ONBOARDING_ICT_DEADLINE_HOURS` | 48 |
| Gaps before management tiers 1..N (hours) | `LANGO_ONBOARDING_ESCALATION_TIER_HOURS` | `24,24` |
| Amendment window (hours) | `LANGO_ONBOARDING_AMENDMENT_DEADLINE_HOURS` | 168 |
| Returns allowed | `LANGO_ONBOARDING_MAX_AMENDMENT_CYCLES` | 3 |

## Requirements

- JDK 21
- Maven 3.9+ (or use the bundled `./mvnw`, which downloads Maven 3.9.11)
- For manual testing only: the [Temporal CLI](https://docs.temporal.io/cli) (or Docker)

## Site visit cycles

The spec lists "Visit Cycles" as a worker job and defines the data (`site_visits.visit_cycles`,
`visits`, `visit_records`, `visit_waivers`, `frequency_overrides`), but not the workflow, so
this is designed here and **the Go backend has to implement its side**.

One workflow runs per `visit_cycles` row (id `visit-cycle-<cycleId>`). The backend starts it,
for example when `ProvisionMerchant` activates a merchant, and stays the only writer:

```
wait until the next due date ─▶ ScheduleVisit (backend creates the visit, returns visit_id)
   ─▶ wait for visitOutcome until the window closes (reminder N days before it closes)
   ─▶ no outcome: MarkVisitMissed ─▶ continue-as-new with the next visit ─▶ ...
                                     until cycleDeactivated
```

- **Fixed schedule.** Visit n is due on `anchor_date + n × interval_days` (n = 0 is the
  anchor date itself), so a late or missed visit never shifts the ones after it.
- **Windows.** A visit opens at midnight on its due date (in `lango.visits.zone`) and stays open
  for `window-days`. `COMPLETED`, `WAIVED` or `CANCELLED` closes it; otherwise it's `MISSED`.
- **Catching up.** Visits whose whole window is already in the past (an anchor in the past, a
  worker outage) are skipped, not created and marked missed at once.
- **Frequency changes** (`frequencyChanged`) take effect straight away while waiting, or after
  the visit that's open. The next visit is always after the last one.
- **Runs for years.** Each run handles one visit and then continues-as-new, so the history stays small.

| Contract | Name | Payload |
|---|---|---|
| Start | `VisitCycleWorkflow` | `VisitCycleWorkflowInput` (proto): `cycleId`, `merchantId`, `outletId`, `regionCode`, `branchCode`, `intervalDays`, `anchorDate` |
| Signal | `visitOutcome` | `VisitOutcomeSignal`: `visitId`, `outcome` = `COMPLETED` / `WAIVED` / `CANCELLED`. Outcomes for any other visit are ignored. |
| Signal | `frequencyChanged` | `VisitFrequencyChangedSignal`: new `intervalDays`, optional new `anchorDate` |
| Signal | `cycleDeactivated` | `VisitCycleDeactivatedSignal`. Ends the workflow; the backend cancels any open visit. |
| Query | `getVisitCycleDiagnostics` | `VisitCycleDiagnostics` |
| Callback | `VisitCallbackService.ScheduleVisit` | backend creates the visit and returns its id. **Must be idempotent on (cycle_id, due_date)**, since a retried call must return the same visit. |
| Callback | `VisitCallbackService.RemindVisitDue` | window about to close, visit not done |
| Callback | `VisitCallbackService.MarkVisitMissed` | window closed, backend sets `MISSED` |

**Policy** (`lango.visits.*`, **placeholders until the business confirms them**; read at the
start of each run, so a change applies from each cycle's next visit):

| Setting | Env var | Default |
|---|---|---|
| Window length (days) | `LANGO_VISITS_WINDOW_DAYS` | 7 |
| Reminder before the window closes (days, 0 = none) | `LANGO_VISITS_REMINDER_DAYS` | 2 |
| Time zone of due dates | `LANGO_VISITS_ZONE` | `Africa/Nairobi` |

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
| `MerchantOnboardingWorkflowTest` (Kotlin) | Onboarding, started with a plain map exactly like Go: approved at every stage is provisioned and activated; an overdue stage alerts its owners at the deadline and then each tier; an open linked ticket pauses the deadline; return and resubmit go back to review; no resubmission in time, or one return too many, declines via the backend; a reviewer's decline ends it; stale or invalid signals are ignored. |
| `OnboardingActivitiesImplTest` (Java) | The policy comes from config; every field of the three callbacks reaches the backend; rejections and outages are retryable. |
| `VisitCycleWorkflowTest` (Kotlin) | Visit cycles over months of skipped time: each visit is created at the start of its due date and the cycle carries on across continue-as-new; no outcome means a reminder and then MISSED, without shifting the schedule; a waiver closes a visit; outcomes for other visits are ignored; visits whose window is already over are skipped; a frequency change reschedules, and never reopens the visit just done; deactivation ends the cycle. |
| `VisitActivitiesImplTest` (Java) | The policy comes from config; every field of the three callbacks reaches the backend; rejections and outages are retryable. |
| `WorkerWiringTest` (`@QuarkusTest`) | The whole app wired together: each workflow is picked up from its own queue (`lango-tickets-tq`, `lango-onboarding-tq`, `lango-visits-tq`), the real activity beans run, and their `@GrpcClient` reaches a fake Go backend on port 19090. |

To run a single test: `./mvnw test -Dtest=MerchantOnboardingWorkflowTest`

## 2. Manual testing against a real Temporal server

**a) Start Temporal with the Lango search attributes** (the workflow fails if they don't exist):

```bash
temporal server start-dev \
  --search-attribute LangoRegion=Keyword --search-attribute LangoBranch=Keyword \
  --search-attribute LangoTicketType=Keyword --search-attribute LangoTicketStatus=Keyword \
  --search-attribute LangoSlaStatus=Keyword --search-attribute LangoSlaDate=Datetime \
  --search-attribute LangoEscalationTier=Int --search-attribute LangoOnboardingStage=Keyword \
  --search-attribute LangoNextVisitDue=Datetime
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

**d) Walk a recruitment application through onboarding.** The start input is plain JSON,
exactly as Go sends it (no `--input-meta`); signals are protobuf like the ticket ones:

```bash
# Start it the way RecruitMerchant does
temporal workflow start --task-queue lango-onboarding-tq --type MerchantOnboardingWorkflow \
  --workflow-id onboarding-demo-1 \
  --input '{"applicationId":"demo-app-1","ticketId":"demo-ticket-1","ticketNumber":"RS10000000031772654400","recruitingAgentId":"agent-1","branchCode":"001","regionCode":"NRB"}'

# Check it (repeat after each step below)
temporal workflow query --workflow-id onboarding-demo-1 --name getOnboardingDiagnostics

# Branch supervisor returns it, the agent resubmits (amendmentCycleCount becomes 1)
temporal workflow signal --workflow-id onboarding-demo-1 --name stageDecision \
  --input-meta encoding=json/protobuf --input-meta messageType=com.tracom.lango.proto.v1.OnboardingStageDecisionSignal \
  --input '{"stage":"AWAITING_REVIEW","outcome":"RETURN","actorId":"supervisor-1","remarks":"KRA PIN missing"}'
temporal workflow signal --workflow-id onboarding-demo-1 --name amendmentResubmitted \
  --input-meta encoding=json/protobuf --input-meta messageType=com.tracom.lango.proto.v1.AmendmentResubmittedSignal \
  --input '{"actorId":"agent-1"}'

# Supervisor approves -> COMPLIANCE_REVIEW
temporal workflow signal --workflow-id onboarding-demo-1 --name stageDecision \
  --input-meta encoding=json/protobuf --input-meta messageType=com.tracom.lango.proto.v1.OnboardingStageDecisionSignal \
  --input '{"stage":"AWAITING_REVIEW","outcome":"APPROVE","actorId":"supervisor-1"}'

# A site inspection ticket is linked (isStageClockPaused becomes true), then resolved
temporal workflow signal --workflow-id onboarding-demo-1 --name linkedTicketOpened \
  --input-meta encoding=json/protobuf --input-meta messageType=com.tracom.lango.proto.v1.LinkedTicketSignal \
  --input '{"ticketId":"inspection-1","linkRelationship":"SITE_INSPECTION","actorId":"compliance-1"}'
temporal workflow signal --workflow-id onboarding-demo-1 --name linkedTicketResolved \
  --input-meta encoding=json/protobuf --input-meta messageType=com.tracom.lango.proto.v1.LinkedTicketSignal \
  --input '{"ticketId":"inspection-1","linkRelationship":"SITE_INSPECTION","actorId":"officer-1"}'

# Compliance approves -> ICT_PROVISIONING; ICT approves -> ProvisionMerchant
temporal workflow signal --workflow-id onboarding-demo-1 --name stageDecision \
  --input-meta encoding=json/protobuf --input-meta messageType=com.tracom.lango.proto.v1.OnboardingStageDecisionSignal \
  --input '{"stage":"COMPLIANCE_REVIEW","outcome":"APPROVE","actorId":"compliance-1"}'
temporal workflow signal --workflow-id onboarding-demo-1 --name stageDecision \
  --input-meta encoding=json/protobuf --input-meta messageType=com.tracom.lango.proto.v1.OnboardingStageDecisionSignal \
  --input '{"stage":"ICT_PROVISIONING","outcome":"APPROVE","actorId":"ict-1"}'

# Find applications by stage (also works as a filter in the Web UI)
temporal workflow list --query 'WorkflowType="MerchantOnboardingWorkflow" AND LangoOnboardingStage="ICT_PROVISIONING"'
```

**e) Start a visit cycle.** All payloads are protobuf. With today's date as the anchor the first
visit is due straight away; with tomorrow's, the cycle waits:

```bash
temporal workflow start --task-queue lango-visits-tq --type VisitCycleWorkflow \
  --workflow-id visit-cycle-demo-1 \
  --input-meta encoding=json/protobuf \
  --input-meta messageType=com.tracom.lango.proto.v1.VisitCycleWorkflowInput \
  --input '{"cycleId":"demo-cycle-1","merchantId":"merchant-1","outletId":"outlet-1","regionCode":"NRB","branchCode":"001","intervalDays":30,"anchorDate":"2026-11-19"}'

# Check it: phase, next due date, open visit
temporal workflow query --workflow-id visit-cycle-demo-1 --name getVisitCycleDiagnostics

# Change the frequency (every 14 days from a new anchor)
temporal workflow signal --workflow-id visit-cycle-demo-1 --name frequencyChanged \
  --input-meta encoding=json/protobuf --input-meta messageType=com.tracom.lango.proto.v1.VisitFrequencyChangedSignal \
  --input '{"intervalDays":14,"anchorDate":"2026-11-21","actorId":"admin-1"}'

# Close the open visit (use currentVisitId from the query)
temporal workflow signal --workflow-id visit-cycle-demo-1 --name visitOutcome \
  --input-meta encoding=json/protobuf --input-meta messageType=com.tracom.lango.proto.v1.VisitOutcomeSignal \
  --input '{"visitId":"<currentVisitId>","outcome":"COMPLETED","actorId":"officer-1"}'

# Deactivate the cycle (ends the workflow)
temporal workflow signal --workflow-id visit-cycle-demo-1 --name cycleDeactivated \
  --input-meta encoding=json/protobuf --input-meta messageType=com.tracom.lango.proto.v1.VisitCycleDeactivatedSignal \
  --input '{"actorId":"admin-1","reason":"merchant closed"}'

# Cycles with a visit due by a given time (UTC)
temporal workflow list --query 'WorkflowType="VisitCycleWorkflow" AND LangoNextVisitDue <= "2026-11-30T00:00:00Z"'
```

Notes:
- Without the Go backend, `ScheduleVisit` keeps retrying when a visit falls due, so the cycle
  stays `WAITING_FOR_DUE_DATE`. Visits that take days or weeks are covered by the automated tests.
- Without the Go backend, `ProvisionMerchant` keeps retrying and the application stays in
  `ICT_PROVISIONING` (pending activity in the Web UI). With the backend it becomes `ACTIVATED`
  and the workflow ends. Remove a demo with `temporal workflow terminate --workflow-id onboarding-demo-1`.
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
| `LANGO_ONBOARDING_*` | see [Merchant onboarding](#merchant-onboarding) | Onboarding deadlines and limits |
| `LANGO_VISITS_*` | see [Site visit cycles](#site-visit-cycles) | Visit window, reminder, time zone |

**Every Temporal namespace** (staging, production) needs all nine `Lango*` search
attributes from section 2a created before the worker is deployed, or the workflows fail.

## Where this differs from the v3.2 spec, and why

1. **Annotations.** The spec uses `@WorkflowImpl(taskQueues=…)` and `@ActivityImpl(taskQueues=…)`,
   which don't exist in the quarkus-temporal extension. Here the ticket workflow and
   activities go on the default worker (`quarkus.temporal.worker.task-queue`); onboarding
   classes opt into the `onboarding` worker with `@TemporalWorkflow` / `@TemporalActivity(workers = "onboarding")`.
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

7. **Onboarding is designed here.** The spec only defines how it's started, its stages and
   roles. The signals, deadlines, callbacks and the new `LangoOnboardingStage` search
   attribute come from this project (see [Merchant onboarding](#merchant-onboarding)).
8. **Visit cycles are designed here.** The spec only defines the tables and permissions. The
   workflow, signals, callbacks, the `lango.visits.*` policy and the `LangoNextVisitDue`
   search attribute come from this project (see [Site visit cycles](#site-visit-cycles)).

## Before deploying onboarding and visit cycles

- **Check the backlog.** Applications Go has already started are waiting on
  `lango-onboarding-tq` and will all run as soon as this worker polls it:
  `temporal workflow list --query 'WorkflowType="MerchantOnboardingWorkflow" AND ExecutionStatus="Running"'`.
  Decide whether to let them run or terminate them first.
- **Create `LangoOnboardingStage`** (Keyword) and **`LangoNextVisitDue`** (Datetime) in every namespace.
- **Confirm the policy numbers** (onboarding and visits) with the business.
- **Start cycles for merchants that are already active.** Only new activations start one
  automatically, so existing `visit_cycles` rows need a one-off script that starts a
  `VisitCycleWorkflow` for each (id `visit-cycle-<cycleId>`, so running it twice is harmless).

## Not in this project yet

- **The Go side of onboarding:** implement `OnboardingCallbackService` on port 9090, and send
  `stageDecision`, `amendmentResubmitted` and `linkedTicketOpened` / `linkedTicketResolved`
  from the review, resubmission and linked-ticket handlers. Also check the start error in
  `RecruitMerchant`, which the spec currently ignores.
- **Recruitment ticket SLA.** `RecruitMerchant` creates a ticket with a 72h SLA but starts no
  `TicketLifecycleWorkflow` for it, and sets its SRM to the recruiting agent.
- **The Go side of visit cycles:** implement `VisitCallbackService` (with `ScheduleVisit`
  idempotent on cycle and due date), start a `VisitCycleWorkflow` when a merchant is activated,
  and send `visitOutcome` (record submitted, waiver stored, visit cancelled), `frequencyChanged`
  (when `frequency_overrides` change) and `cycleDeactivated`.
- The Go side of `RecordSlaBreachAndEscalate` (writing the breach and calling the notification
  service) lives in the backend, not here.
