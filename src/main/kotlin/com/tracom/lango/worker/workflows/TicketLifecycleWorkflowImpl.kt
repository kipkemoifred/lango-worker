package com.tracom.lango.worker.workflows

import com.tracom.lango.proto.v1.PauseSlaSignal
import com.tracom.lango.proto.v1.ResumeSlaSignal
import com.tracom.lango.proto.v1.StatusTransitionSignal
import com.tracom.lango.proto.v1.TicketWorkflowDiagnostics
import com.tracom.lango.proto.v1.TicketWorkflowInput
import com.tracom.lango.worker.activities.TicketActivities
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import io.temporal.common.SearchAttributeKey
import io.temporal.workflow.Async
import io.temporal.workflow.Promise
import io.temporal.workflow.Workflow
import java.time.Duration

/**
 * SLA countdown + warning + escalation ladder for one ticket.
 *
 * Bound to the default worker; its task queue (lango-tickets-tq) is set in
 * application.properties via quarkus.temporal.worker.task-queue.
 *
 * BA data flow DFD-2 (all optional in the input; without them the ticket behaves as before):
 *  - Warning (F20): when sla_warning_percent of the SLA is used, the SRM is warned once.
 *    Pauses push it back like the deadline; a ticket resolved first is never warned.
 *  - Calendar (F04): if the backend sends a deadline it computed in business time, the clock
 *    runs to it, and a resume may carry a new one. Otherwise sla_hours_applied is counted in
 *    clock hours. Escalation gaps are always clock hours: a late ticket doesn't wait for Monday.
 *  - Reopen (2.5): REOPENED after RESOLVED continues the remaining SLA, and the escalation
 *    ladder if it had already breached. After CLOSED the backend starts a new run carrying
 *    the closed run's state (ReopenedTicketState) within its reopen window.
 *
 * Differences from the v3.2 spec (deliberate fixes):
 *  - No activity calls inside signal handlers. Handlers only change state; the paused total
 *    and the warning go to the backend in the background, so the SLA clock never waits on them.
 *  - Activities retry without an attempt cap (max 1 min between tries). With the
 *    spec's cap of 10, a backend outage of ~9 minutes would fail the workflow and
 *    silently stop all escalations for that ticket.
 *  - Pauses received after the SLA is already breached are ignored.
 *  - The diagnostics query reports live remaining time and the SLA deadline, not the
 *    value from the last timer wake-up.
 */
class TicketLifecycleWorkflowImpl : TicketLifecycleWorkflow {

    companion object {
        const val TASK_QUEUE = "lango-tickets-tq"

        val ATTR_TICKET_STATUS: SearchAttributeKey<String> = SearchAttributeKey.forKeyword("LangoTicketStatus")
        val ATTR_SLA_STATUS: SearchAttributeKey<String> = SearchAttributeKey.forKeyword("LangoSlaStatus")
        val ATTR_ESCALATION_TIER: SearchAttributeKey<Long> = SearchAttributeKey.forLong("LangoEscalationTier")

        private const val RESOLVED = "RESOLVED"
        private const val REOPENED = "REOPENED"
        private val STOPS_THE_CLOCK = setOf(RESOLVED, "CLOSED", "DECLINED")
        private val ENDS_THE_WORKFLOW = setOf("CLOSED", "DECLINED")
    }

    private val log = Workflow.getLogger(TicketLifecycleWorkflowImpl::class.java)

    private val activities: TicketActivities = Workflow.newActivityStub(
        TicketActivities::class.java,
        ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofSeconds(15))
            .setRetryOptions(
                RetryOptions.newBuilder()
                    .setInitialInterval(Duration.ofSeconds(1))
                    .setBackoffCoefficient(2.0)
                    .setMaximumInterval(Duration.ofMinutes(1))
                    // no maximum attempts: keep retrying until the backend is back
                    .build()
            )
            .build()
    )

    // --- state (rebuilt deterministically on replay) ---
    private var ticketId = ""
    private var ticketNumber = ""
    private var srmUserId = ""
    private var currentStatus = "RAISED"
    private var escalationTierHours: List<Long> = emptyList()

    private var totalSlaMs = 0L
    private var remainingSlaMs = 0L
    private var pendingDueAtMs = 0L
    private var clockRunning = false
    private var clockStartedAtMs = 0L
    private var slaBreached = false
    private var nextTier = 0
    private var currentEscalationTier = 0

    private var warningPercent = 0
    private var warningSent = false
    private var reopenCount = 0

    private var isPaused = false
    private var pausedAtMs = 0L
    private var totalPausedMs = 0L
    private var pausedTotalNeedsSync = false
    private var lastResumeActorId = ""
    private var syncBusy = false
    private var finished = false
    private val callbacksInFlight = mutableListOf<Promise<Void>>()

    override fun execute(input: TicketWorkflowInput) {
        ticketId = input.ticketId
        ticketNumber = input.ticketNumber
        srmUserId = input.srmUserId
        escalationTierHours = input.escalationTierHoursList
        warningPercent = input.slaWarningPercent.coerceIn(0, 100)
        remainingSlaMs = Duration.ofHours(input.slaHoursApplied).toMillis()
        totalSlaMs = remainingSlaMs
        if (input.slaDueAtEpochMs > 0) {
            val reportedAtMs = if (input.reportedAtEpochMs > 0) input.reportedAtEpochMs else Workflow.currentTimeMillis()
            if (input.slaHoursApplied == 0L) totalSlaMs = (input.slaDueAtEpochMs - reportedAtMs).coerceAtLeast(0L)
            pendingDueAtMs = input.slaDueAtEpochMs
        }
        if (input.hasReopened()) restoreReopened(input)

        log.info(
            "Started ticketNum={} with SLA={}h due={} tiers={} warning={}% reopens={}",
            ticketNumber, input.slaHoursApplied, input.slaDueAtEpochMs, escalationTierHours, warningPercent, reopenCount
        )
        Async.procedure { reportPausedTimeInBackground() }

        // Runs until CLOSED or DECLINED. RESOLVED stops the clock, but a REOPENED brings it back.
        while (currentStatus !in ENDS_THE_WORKFLOW) {
            when {
                clockStopped() -> Workflow.await { !clockStopped() || currentStatus in ENDS_THE_WORKFLOW }
                !slaBreached -> runSlaClock()
                else -> runEscalation()
            }
        }

        // Let the background calls deliver the final paused total and any warning, so none is lost.
        finished = true
        Workflow.await { !syncBusy && !pausedTotalNeedsSync }
        Promise.allOf(callbacksInFlight).get()
        log.info("Finished ticketNum={} status={}", ticketNumber, currentStatus)
    }

    override fun pauseSla(signal: PauseSlaSignal) {
        if (isPaused || clockStopped() || slaBreached) return
        isPaused = true
        pausedAtMs = Workflow.currentTimeMillis()
        log.info("SLA paused for ticketNum={} by {}: {}", ticketNumber, signal.actorId, signal.reason)
    }

    override fun resumeSla(signal: ResumeSlaSignal) {
        if (!isPaused) return
        closeOpenPause(signal.actorId)
        if (signal.slaDueAtEpochMs > 0) pendingDueAtMs = signal.slaDueAtEpochMs
        log.info("SLA resumed for ticketNum={}; totalPausedSecs={}", ticketNumber, totalPausedMs / 1000)
    }

    override fun onStatusTransition(signal: StatusTransitionSignal) {
        if (signal.newStatus == REOPENED) {
            if (currentStatus != RESOLVED) {
                log.warn("Ignoring REOPENED by {}: ticketNum={} is {}", signal.actorId, ticketNumber, currentStatus)
                return
            }
            reopenCount += 1
            if (signal.slaDueAtEpochMs > 0) pendingDueAtMs = signal.slaDueAtEpochMs
            log.info("Ticket reopened: ticketNum={} by {} (reopen {})", ticketNumber, signal.actorId, reopenCount)
        }
        if (isPaused && signal.newStatus in STOPS_THE_CLOCK) {
            closeOpenPause(signal.actorId)
        }
        currentStatus = signal.newStatus
        Workflow.upsertTypedSearchAttributes(ATTR_TICKET_STATUS.valueSet(currentStatus))
    }

    override fun getWorkflowDiagnostics(): TicketWorkflowDiagnostics {
        // In a query the workflow clock is the time of the last event, so it stands still between
        // events; the wall clock gives a live countdown. (With skipped time in tests the workflow
        // clock runs ahead of the wall clock, hence the max.) Queries change nothing, so this is safe.
        val now = maxOf(Workflow.currentTimeMillis(), System.currentTimeMillis())
        val livePausedMs = if (isPaused) totalPausedMs + (now - pausedAtMs).coerceAtLeast(0L) else totalPausedMs
        val liveRemainingMs =
            if (clockRunning) (remainingSlaMs - (now - clockStartedAtMs)).coerceAtLeast(0L) else remainingSlaMs

        return TicketWorkflowDiagnostics.newBuilder()
            .setTicketId(ticketId)
            .setTicketNumber(ticketNumber)
            .setCurrentStatus(currentStatus)
            .setSlaStatus(if (slaBreached) "PAST_SLA" else "WITHIN_SLA")
            .setIsSlaPaused(isPaused)
            .setAccumulatedPausedSeconds(livePausedMs / 1000)
            .setRemainingSlaSeconds(liveRemainingMs / 1000)
            .setSlaDueAtEpochMs(if (clockRunning) clockStartedAtMs + remainingSlaMs else 0L)
            .setCurrentEscalationTier(currentEscalationTier)
            .setSlaWarningSent(warningSent)
            .setReopenCount(reopenCount)
            .build()
    }

    // --- phases ---

    /** Counts down the SLA (stopping while paused), warns at the warning point, and marks the breach. */
    private fun runSlaClock() {
        while (!clockStopped() && !slaBreached) {
            if (isPaused) {
                Workflow.await { !isPaused || clockStopped() }
                continue
            }
            applyPendingDeadline()
            if (remainingSlaMs == 0L) {
                slaBreached = true
                break
            }
            if (warningDueNow()) sendWarning()

            val warnInMs = msUntilWarning()
            clockStartedAtMs = Workflow.currentTimeMillis()
            clockRunning = true
            val wokenEarly = Workflow.await(Duration.ofMillis(warnInMs ?: remainingSlaMs)) {
                isPaused || clockStopped() || pendingDueAtMs != 0L
            }
            clockRunning = false

            val elapsedMs = (Workflow.currentTimeMillis() - clockStartedAtMs).coerceAtLeast(0L)
            remainingSlaMs = (remainingSlaMs - elapsedMs).coerceAtLeast(0L)
            if (!wokenEarly) {
                if (warnInMs != null) {
                    sendWarning()
                } else {
                    remainingSlaMs = 0L
                    slaBreached = true
                }
            }
        }
    }

    /** Alerts the SRM (tier 0) on the breach, then climbs the management tiers until the ticket is resolved. */
    private fun runEscalation() {
        if (nextTier == 0) {
            log.info("SLA breached for ticketNum={}", ticketNumber)
            Workflow.upsertTypedSearchAttributes(ATTR_SLA_STATUS.valueSet("PAST_SLA"))
            escalate(0)
            nextTier = 1
        }
        while (!clockStopped() && nextTier <= escalationTierHours.size) {
            val resolvedInTime = Workflow.await(Duration.ofHours(escalationTierHours[nextTier - 1])) { clockStopped() }
            if (resolvedInTime) return

            currentEscalationTier = nextTier
            Workflow.upsertTypedSearchAttributes(ATTR_ESCALATION_TIER.valueSet(currentEscalationTier.toLong()))
            escalate(currentEscalationTier)
            nextTier += 1
        }
        // every tier has been alerted: wait for the ticket to be resolved or closed
        Workflow.await { clockStopped() }
    }

    // --- helpers ---

    private fun clockStopped() = currentStatus in STOPS_THE_CLOCK

    /** A ticket reopened after CLOSED: carry on from where its closed run ended. */
    private fun restoreReopened(input: TicketWorkflowInput) {
        val state = input.reopened
        if (input.slaDueAtEpochMs == 0L) remainingSlaMs = Duration.ofSeconds(state.remainingSlaSeconds).toMillis()
        slaBreached = state.slaBreached
        currentEscalationTier = state.escalationTier
        nextTier = if (state.slaBreached) state.escalationTier + 1 else 0
        totalPausedMs = Duration.ofSeconds(state.totalPausedSeconds).toMillis()
        warningSent = state.slaWarningSent
        reopenCount = state.reopenCount + 1
        currentStatus = REOPENED
        Workflow.upsertTypedSearchAttributes(ATTR_TICKET_STATUS.valueSet(currentStatus))
        if (slaBreached) Workflow.upsertTypedSearchAttributes(ATTR_SLA_STATUS.valueSet("PAST_SLA"))
    }

    /** A deadline sent by the backend (business time) replaces the remaining clock time. */
    private fun applyPendingDeadline() {
        if (pendingDueAtMs == 0L) return
        remainingSlaMs = (pendingDueAtMs - Workflow.currentTimeMillis()).coerceAtLeast(0L)
        pendingDueAtMs = 0L
    }

    private fun warningThresholdMs(): Long = totalSlaMs * (100 - warningPercent) / 100

    /** Past the warning point already (for example a reopened ticket with little SLA left). */
    private fun warningDueNow(): Boolean =
        warningPercent > 0 && !warningSent && remainingSlaMs <= warningThresholdMs()

    /** Clock time until the warning point, or null when no warning is still to come. */
    private fun msUntilWarning(): Long? {
        if (warningPercent == 0 || warningSent) return null
        val untilMs = remainingSlaMs - warningThresholdMs()
        return if (untilMs > 0) untilMs else null
    }

    /** Warns the SRM once, in the background, so a backend outage can't delay the breach. */
    private fun sendWarning() {
        warningSent = true
        val warnedAtMs = Workflow.currentTimeMillis()
        val dueAtMs = warnedAtMs + remainingSlaMs
        log.info("SLA warning for ticketNum={} at {}% used", ticketNumber, warningPercent)
        callbacksInFlight += Async.procedure {
            activities.recordSlaWarning(ticketId, ticketNumber, srmUserId, warningPercent, warnedAtMs, dueAtMs)
        }
    }

    private fun closeOpenPause(actorId: String) {
        totalPausedMs += (Workflow.currentTimeMillis() - pausedAtMs).coerceAtLeast(0L)
        isPaused = false
        lastResumeActorId = actorId
        pausedTotalNeedsSync = true
    }

    /**
     * Sends the paused total to the backend whenever a pause ends: one call at a time, always the
     * latest total. Runs beside the main loop, so a backend outage never holds up the SLA clock.
     */
    private fun reportPausedTimeInBackground() {
        while (true) {
            Workflow.await { pausedTotalNeedsSync || finished }
            if (!pausedTotalNeedsSync) return
            pausedTotalNeedsSync = false
            syncBusy = true
            activities.syncPausedDurationToBackend(ticketId, totalPausedMs / 1000, lastResumeActorId)
            syncBusy = false
        }
    }

    private fun escalate(tier: Int) {
        activities.recordAndNotifySlaEscalation(
            ticketId, ticketNumber, tier, srmUserId, Workflow.currentTimeMillis(), totalPausedMs / 1000
        )
    }
}
