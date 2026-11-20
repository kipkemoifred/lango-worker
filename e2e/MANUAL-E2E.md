# Manual end-to-end testing

This walks through all three workflows against a **real** Temporal server, the **real**
worker, and a **stub Go backend** that prints every callback the worker makes. You play the
Go backend's other half (starting workflows, sending signals) with the `temporal` CLI.

```
 you (temporal CLI)  ──start / signal / query──▶  Temporal server  ◀──polls──  lango-worker
   "the Go backend's                              localhost:7233                │
    API handlers"                                 UI: localhost:8233            │ gRPC callbacks
                                                                                ▼
                                                              e2e/StubGoBackend.java  :9090
                                                              "the Go backend's internal server"
```

What "end to end" proves here that `./mvnw test` doesn't: real timers on a real server,
real protobuf payloads over the wire, the worker's real gRPC client reaching a real socket,
retries when the backend is down, and workflows surviving a worker restart.

---

## 0. One-time setup

```bash
./mvnw package -DskipTests        # builds target/quarkus-app (the stub borrows its jars)
```

Re-run this after changing the code if you use the packaged worker (step 3b). Dev mode
(step 3a) recompiles on its own.

You need four terminals. Run everything from the project root.

| Terminal | Runs |
|---|---|
| T1 | Temporal dev server |
| T2 | Stub Go backend (watch this one: it's where callbacks show up) |
| T3 | The worker |
| T4 | Your commands |

---

## 1. T1: Temporal server

```bash
temporal server start-dev \
  --search-attribute LangoRegion=Keyword --search-attribute LangoBranch=Keyword \
  --search-attribute LangoTicketType=Keyword --search-attribute LangoTicketStatus=Keyword \
  --search-attribute LangoSlaStatus=Keyword --search-attribute LangoSlaDate=Datetime \
  --search-attribute LangoEscalationTier=Int --search-attribute LangoOnboardingStage=Keyword \
  --search-attribute LangoNextVisitDue=Datetime
```

The workflows write these search attributes; if one is missing the workflow task fails.
Web UI: <http://localhost:8233>. The dev server keeps state in memory, so restarting it
wipes everything.

## 2. T2: Stub Go backend

```bash
java -cp "target/classes:target/quarkus-app/lib/main/*" e2e/StubGoBackend.java
```

It answers all eight callback RPCs with `success=true` and prints one line per call:

```
13:00:11  RecordSlaBreachAndEscalate     ticket_id: "e2e-1" ticket_number: "RS..." srm_user_id: "srm-1" breached_at_epoch_ms: ...
```

- Ignore the SLF4J warnings at startup; they're harmless.
- **Fields with their default value aren't printed** (protobuf text format), so
  `escalation_tier: 0` (the SRM / stage owners) shows up as no `escalation_tier` at all.
- `STUB_REJECT=true java ...` makes it answer `success=false` (see section 7).
- `STUB_PORT=9191` changes the port (then point the worker at it with
  `GRPC_CLIENTS_LANGO_GO_BACKEND_PORT=9191`).

## 3. T3: The worker — pick one mode

### 3a. Realistic mode (default namespace, real policy)

```bash
./mvnw quarkus:dev -Dquarkus.temporal.connection.target=localhost:7233
```

Uses the real policy: onboarding stages take 24–72 h to go overdue, visit windows are 7 days.
Good for walking the happy paths and the signal logic.

### 3b. Fast mode (separate `e2e` namespace, zero-hour deadlines)

The policy is in whole hours/days, so to *see* overdue alerts and expiries without waiting,
run a second worker with the deadlines set to 0, in its own namespace so it never steals
work from 3a. Create the namespace once (while T1 runs):

```bash
temporal operator namespace create --namespace e2e
for sa in LangoRegion=Keyword LangoBranch=Keyword LangoTicketType=Keyword LangoTicketStatus=Keyword \
          LangoSlaStatus=Keyword LangoSlaDate=Datetime LangoEscalationTier=Int \
          LangoOnboardingStage=Keyword LangoNextVisitDue=Datetime; do
  temporal operator search-attribute create --namespace e2e --name "${sa%=*}" --type "${sa#*=}"
done
```

Then run the packaged worker against it:

```bash
QUARKUS_TEMPORAL_NAMESPACE=e2e \
QUARKUS_TEMPORAL_CONNECTION_TARGET=localhost:7233 \
QUARKUS_HTTP_PORT=8082 \
LANGO_ONBOARDING_REVIEW_DEADLINE_HOURS=0 \
LANGO_ONBOARDING_ESCALATION_TIER_HOURS=0,0 \
LANGO_ONBOARDING_AMENDMENT_DEADLINE_HOURS=0 \
LANGO_VISITS_WINDOW_DAYS=1 \
LANGO_VISITS_REMINDER_DAYS=0 \
java -jar target/quarkus-app/quarkus-run.jar
```

What each override does:

| Override | Effect |
|---|---|
| `REVIEW_DEADLINE_HOURS=0` | `AWAITING_REVIEW` is overdue the moment the application starts |
| `ESCALATION_TIER_HOURS=0,0` | tiers 1 and 2 follow immediately after tier 0 |
| `AMENDMENT_DEADLINE_HOURS=0` | a `RETURN` declines straight away with `AMENDMENT_EXPIRED` |
| `VISITS_WINDOW_DAYS=1`, `REMINDER_DAYS=0` | a visit due today closes at midnight; no reminder |

You can run 3a and 3b at the same time (they use ports 8081 and 8082). Every command aimed
at the fast worker needs `--namespace e2e`.

> The policy is read **when a workflow starts**. Changing an env var and restarting the
> worker only affects workflows started afterwards.

---

## 4. T4: A helper to keep commands short

All payloads are protobuf, so every start and signal needs two `--input-meta` flags naming
the message type. Paste these into T4 once:

```bash
P=com.tracom.lango.proto.v1
NS=default   # set NS=e2e for the fast worker

start() {  # start <queue> <type> <workflow-id> <input message> <json>
  temporal workflow start --namespace "$NS" --task-queue "$1" --type "$2" --workflow-id "$3" \
    --input-meta encoding=json/protobuf --input-meta messageType=$P.$4 --input "$5"
}
sig() {    # sig <workflow-id> <signal name> <message> <json>
  temporal workflow signal --namespace "$NS" --workflow-id "$1" --name "$2" \
    --input-meta encoding=json/protobuf --input-meta messageType=$P.$3 --input "$4"
}
q() {      # q <workflow-id> <query name>
  temporal workflow query --namespace "$NS" --workflow-id "$1" --name "$2"
}
```

JSON field names are the camelCase form of the proto fields (`sla_due_at_epoch_ms` →
`slaDueAtEpochMs`). `int64` fields can be sent as strings or numbers. In query results,
fields that are 0 / false / empty are left out.

---

## 5. Ticket SLA (`lango-tickets-tq`, query `getWorkflowDiagnostics`)

Works in either namespace; ticket timing comes from the input, not the policy.

### 5.1 Breach in 90 seconds, with a warning at 50 %

`slaHoursApplied` is in whole hours, but `slaDueAtEpochMs` (the backend's business-time
deadline) can be anything. With `slaHoursApplied` = 0, the SLA length is
`slaDueAtEpochMs − reportedAtEpochMs`, so the warning point works too.

```bash
NOW=$(date +%s%3N); DUE=$((NOW + 90000))
start lango-tickets-tq TicketLifecycleWorkflow ticket-e2e-1 TicketWorkflowInput \
  "{\"ticketId\":\"e2e-1\",\"ticketNumber\":\"RS10000000099\",\"ticketType\":\"SERVICE\",\"srmUserId\":\"srm-1\",
    \"reportedAtEpochMs\":\"$NOW\",\"slaDueAtEpochMs\":\"$DUE\",\"slaWarningPercent\":50,\"escalationTierHours\":[\"1\"]}"
q ticket-e2e-1 getWorkflowDiagnostics
```

Expect:

| When | Query shows | T2 shows |
|---|---|---|
| right away | `WITHIN_SLA`, `remainingSlaSeconds` ≈ 89 and counting down | nothing |
| ~45 s | `slaWarningSent: true` | `RecordSlaWarning ... warning_percent: 50` |
| ~90 s | `slaStatus: PAST_SLA`, no `remainingSlaSeconds` | `RecordSlaBreachAndEscalate` with no `escalation_tier` (= 0, the SRM) |
| +1 h | `currentEscalationTier: 1` | `RecordSlaBreachAndEscalate ... escalation_tier: 1` |

Also try: `temporal workflow list --query 'LangoSlaStatus="PAST_SLA"'`.

### 5.2 Pause and resume (moves the deadline)

Start another 90 s ticket (`ticket-e2e-2`, as above), then:

```bash
sig ticket-e2e-2 pauseSla PauseSlaSignal '{"actorId":"officer-1","reason":"waiting on merchant"}'
q ticket-e2e-2 getWorkflowDiagnostics    # isSlaPaused: true, slaDueAtEpochMs gone
# wait ~30 s
sig ticket-e2e-2 resumeSla ResumeSlaSignal '{"actorId":"officer-1"}'
q ticket-e2e-2 getWorkflowDiagnostics    # accumulatedPausedSeconds ≈ 30, deadline 30 s later
```

Expect `SyncPausedSlaDuration ... total_paused_seconds: 30 updated_by_actor_id: "officer-1"`
in T2 right after the resume, and the breach ~30 s later than it would have been. Sending
`pauseSla` twice in a row is ignored (no double counting).

### 5.3 Resolve, reopen, close

```bash
sig ticket-e2e-3 statusTransition StatusTransitionSignal '{"newStatus":"RESOLVED","actorId":"officer-1"}'
q ticket-e2e-3 getWorkflowDiagnostics    # clock stopped, remainingSlaSeconds frozen
sig ticket-e2e-3 statusTransition StatusTransitionSignal '{"newStatus":"REOPENED","actorId":"officer-1"}'
q ticket-e2e-3 getWorkflowDiagnostics    # reopenCount: 1, clock running again from where it stopped
sig ticket-e2e-3 statusTransition StatusTransitionSignal '{"newStatus":"CLOSED","actorId":"officer-1"}'
temporal workflow describe --namespace "$NS" --workflow-id ticket-e2e-3   # Status COMPLETED
```

Resolve a ticket **before** its warning point and no warning or breach ever reaches T2.
`DECLINED` ends the workflow like `CLOSED`. `REOPENED` on a ticket that isn't resolved is ignored.

---

## 6. Merchant onboarding (`lango-onboarding-tq`, query `getOnboardingDiagnostics`)

### 6.1 Happy path to ACTIVATED (realistic mode, `NS=default`)

No callbacks happen on this path, so T2 stays quiet. Query after every step and watch
`currentStage` move.

```bash
start lango-onboarding-tq MerchantOnboardingWorkflow onboarding-e2e-1 OnboardingWorkflowInput \
  '{"applicationId":"e2e-app-1","ticketId":"t1","ticketNumber":"RS1","recruitingAgentId":"agent-1","branchCode":"001","regionCode":"NRB"}'
q onboarding-e2e-1 getOnboardingDiagnostics                                   # AWAITING_REVIEW

sig onboarding-e2e-1 stageDecision OnboardingStageDecisionSignal '{"stage":"AWAITING_REVIEW","outcome":"RETURN","actorId":"supervisor-1"}'
sig onboarding-e2e-1 amendmentResubmitted AmendmentResubmittedSignal '{"actorId":"agent-1"}'
q onboarding-e2e-1 getOnboardingDiagnostics                                   # AWAITING_REVIEW, amendmentCycleCount: 1

sig onboarding-e2e-1 stageDecision OnboardingStageDecisionSignal '{"stage":"AWAITING_REVIEW","outcome":"APPROVE","actorId":"supervisor-1"}'    # -> COMPLIANCE_REVIEW
sig onboarding-e2e-1 stageDecision OnboardingStageDecisionSignal '{"stage":"COMPLIANCE_REVIEW","outcome":"APPROVE","actorId":"compliance-1"}' # -> SITE_INSPECTION
sig onboarding-e2e-1 inspectionSubmitted InspectionSubmittedSignal '{"actorId":"agent-1","inspectionId":"insp-1"}'                            # -> INSPECTION_APPROVAL
sig onboarding-e2e-1 stageDecision OnboardingStageDecisionSignal '{"stage":"INSPECTION_APPROVAL","outcome":"RETURN","actorId":"supervisor-1"}' # -> SITE_INSPECTION, inspectionReturnCount: 1
sig onboarding-e2e-1 inspectionSubmitted InspectionSubmittedSignal '{"actorId":"agent-1","inspectionId":"insp-2"}'                            # -> INSPECTION_APPROVAL
sig onboarding-e2e-1 stageDecision OnboardingStageDecisionSignal '{"stage":"INSPECTION_APPROVAL","outcome":"APPROVE","actorId":"supervisor-1"}' # -> ICT_PROVISIONING
```

**Negative checks** before the last step (each one should change nothing; the worker logs
an "Ignoring ..." warning in T3):

```bash
# a decision for a stage it's no longer in (a double click)
sig onboarding-e2e-1 stageDecision OnboardingStageDecisionSignal '{"stage":"AWAITING_REVIEW","outcome":"APPROVE","actorId":"supervisor-1"}'
# ICT approval without MID/TID
sig onboarding-e2e-1 stageDecision OnboardingStageDecisionSignal '{"stage":"ICT_PROVISIONING","outcome":"APPROVE","actorId":"ict-1"}'
q onboarding-e2e-1 getOnboardingDiagnostics                                   # still ICT_PROVISIONING
```

Activate:

```bash
sig onboarding-e2e-1 stageDecision OnboardingStageDecisionSignal \
  '{"stage":"ICT_PROVISIONING","outcome":"APPROVE","actorId":"ict-1","merchantId":"merchant-1","outletId":"outlet-1","mid":"MID000123","tid":"TID000456"}'
temporal workflow describe --namespace "$NS" --workflow-id onboarding-e2e-1   # Status COMPLETED
```

Ordering check: sending two signals back to back (e.g. the compliance APPROVE and the
inspectionSubmitted in one line joined with `&&`) still applies both, in order.

### 6.2 Overdue alerts and worker declines (fast mode, `NS=e2e`)

```bash
NS=e2e
start lango-onboarding-tq MerchantOnboardingWorkflow onboarding-e2e-2 OnboardingWorkflowInput \
  '{"applicationId":"e2e-app-2","ticketId":"t2","ticketNumber":"RS2","recruitingAgentId":"agent-1","branchCode":"001","regionCode":"NRB"}'
```

T2 immediately shows three `NotifyOnboardingStageOverdue` lines for `AWAITING_REVIEW`:
no `escalation_tier` (tier 0, the stage owners), then `escalation_tier: 1` and `2`. The query
shows `stageEscalationTier: 2`. No more alerts follow; the stage just waits.

Now return it. With a 0-hour amendment window the worker declines straight away:

```bash
sig onboarding-e2e-2 stageDecision OnboardingStageDecisionSignal '{"stage":"AWAITING_REVIEW","outcome":"RETURN","actorId":"supervisor-1"}'
```

T2: `DeclineOnboardingApplication ... stage: "RETURNED_FOR_AMENDMENT" reason: "AMENDMENT_EXPIRED"`,
and the workflow is COMPLETED.

To see `MAX_AMENDMENTS_EXCEEDED` instead, restart the fast worker with
`LANGO_ONBOARDING_MAX_AMENDMENT_CYCLES=0` and a normal `LANGO_ONBOARDING_AMENDMENT_DEADLINE_HOURS`
(e.g. 168), start a new application, and `RETURN` it once.

A reviewer's `DECLINE` (`"outcome":"DECLINE"`) ends the workflow **without** a callback,
because the backend already stored it.

---

## 7. Site visit cycles (`lango-visits-tq`, query `getVisitCycleDiagnostics`)

### 7.1 Schedule, complete, next visit

Anchor today, so the first visit is due today and well inside the planning horizon:

```bash
start lango-visits-tq VisitCycleWorkflow visit-cycle-e2e-1 VisitCycleWorkflowInput \
  "{\"cycleId\":\"e2e-cycle-1\",\"merchantId\":\"m1\",\"outletId\":\"o1\",\"regionCode\":\"NRB\",\"branchCode\":\"001\",\"intervalDays\":30,\"anchorDate\":\"$(date +%F)\"}"
q visit-cycle-e2e-1 getVisitCycleDiagnostics
```

T2: `ScheduleVisit cycle_id: "e2e-cycle-1" ... due_date: "<today>"`. The query shows
`phase: VISIT_OPEN` and `currentVisitId: "visit-e2e-cycle-1-<today>"` (the stub builds the
id from cycle + due date, like the idempotent real backend).

```bash
sig visit-cycle-e2e-1 visitOutcome VisitOutcomeSignal "{\"visitId\":\"visit-e2e-cycle-1-$(date +%F)\",\"outcome\":\"COMPLETED\",\"actorId\":\"officer-1\"}"
q visit-cycle-e2e-1 getVisitCycleDiagnostics
```

Expect `lastVisitOutcome: COMPLETED`, `phase: WAITING_TO_SCHEDULE`, `nextDueDate` = today + 30,
`nextScheduleDate` = that − 14 (the planning horizon). In the Web UI the workflow has
**continued-as-new**: a fresh run with the same workflow id.

Negative check: a `visitOutcome` with any other `visitId` is ignored.

### 7.2 Frequency change, deactivate

```bash
sig visit-cycle-e2e-1 frequencyChanged VisitFrequencyChangedSignal "{\"intervalDays\":7,\"anchorDate\":\"$(date -d tomorrow +%F)\",\"actorId\":\"admin-1\"}"
q visit-cycle-e2e-1 getVisitCycleDiagnostics
```

Tomorrow is within the 14-day horizon, so T2 shows a new `ScheduleVisit` for tomorrow at
once and the phase is `VISIT_OPEN` again.

```bash
sig visit-cycle-e2e-1 cycleDeactivated VisitCycleDeactivatedSignal '{"actorId":"admin-1","reason":"merchant closed"}'
```

The workflow ends (COMPLETED). No callback: the backend cancels the open visit itself.

### 7.3 Missed visit

`MarkVisitMissed` fires when a window ends with no outcome. Windows end at midnight
(`Africa/Nairobi`), so in fast mode a visit anchored today is marked missed at the next
midnight. Leave one running overnight, or rely on `VisitCycleWorkflowTest` for that path.
An anchor date far in the past is **skipped** (no ScheduleVisit, no MarkVisitMissed for
windows that already closed); try `"anchorDate":"2026-01-01"` to see it jump straight to
the next future due date.

---

## 8. Resilience checks

These are the most valuable things a manual run can show.

**Backend down.** Stop T2 (Ctrl+C), then trigger a callback (e.g. a 60 s ticket from 5.1).
At the deadline:

```bash
temporal workflow describe --namespace "$NS" --workflow-id ticket-e2e-4
```

shows `Pending Activities: 1`, `Type RecordSlaBreachAndEscalate`, a growing `Attempt` and
`Connection refused ... :9090`; the Web UI shows the same. `MaximumAttempts 0` means unlimited.
The query still says `PAST_SLA`: the SLA clock never waits on the backend. Start T2 again
and the call goes through on the next retry (retries back off to at most 1 minute apart,
and the gRPC channel can take up to ~2 minutes to reconnect).

**Backend says no.** Restart T2 with `STUB_REJECT=true`. Calls arrive in T2 marked
`-> success=false` and keep being retried, exactly like an outage. Restart without it and
they succeed.

**Worker restart.** Start a 3-minute ticket, stop T3 a minute in, wait past the deadline,
start T3 again. The breach fires as soon as the worker is back, and the query shows the
correct state: the timer lived in Temporal, not in the worker.

---

## 9. Finding things

```bash
temporal workflow list --namespace "$NS" --query 'ExecutionStatus="Running"'
temporal workflow list --query 'WorkflowType="MerchantOnboardingWorkflow" AND LangoOnboardingStage="ICT_PROVISIONING"'
temporal workflow list --query 'WorkflowType="VisitCycleWorkflow" AND LangoNextVisitDue <= "2026-12-31T00:00:00Z"'
temporal workflow show --namespace "$NS" --workflow-id ticket-e2e-1     # full event history
```

The same queries work in the Web UI's search box (pick the namespace top left).

## 10. Clean up

```bash
temporal workflow terminate --namespace "$NS" --query 'ExecutionStatus="Running"' --reason "manual e2e done" --yes
```

Stopping T1 wipes everything anyway, since the dev server is in memory.
