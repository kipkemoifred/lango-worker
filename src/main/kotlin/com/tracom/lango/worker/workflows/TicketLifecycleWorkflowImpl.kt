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
import io.temporal.workflow.Workflow
import java.time.Duration

/**
 * SLA countdown + escalation ladder for one ticket.
 *
 * Bound to the default worker; its task queue (lango-tickets-tq) is set in
 * application.properties via quarkus.temporal.worker.task-queue.
 *
 * Differences from the v3.2 spec (deliberate fixes):
 *  - No activity calls inside signal handlers. Handlers only change state; the
 *    main loop does the backend sync afterwards (Temporal's recommended pattern).
 *  - Activities retry without an attempt cap (max 1 min between tries). With the
 *    spec's cap of 10, a backend outage of ~9 minutes would fail the workflow and
 *    silently stop all escalations for that ticket.
 *  - Pauses received after the SLA is already breached are ignored.
 *  - The diagnostics query reports live remaining time, not the value from the
 *    last timer wake-up.
 */
class TicketLifecycleWorkflowImpl : TicketLifecycleWorkflow {

    companion object {
        const val TASK_QUEUE = "lango-tickets-tq"

        val ATTR_TICKET_STATUS: SearchAttributeKey<String> = SearchAttributeKey.forKeyword("LangoTicketStatus")
        val ATTR_SLA_STATUS: SearchAttributeKey<String> = SearchAttributeKey.forKeyword("LangoSlaStatus")
        val ATTR_ESCALATION_TIER: SearchAttributeKey<Long> = SearchAttributeKey.forLong("LangoEscalationTier")

        private val STOPS_THE_CLOCK = setOf("RESOLVED", "CLOSED", "DECLINED")
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

    private var remainingSlaMs = 0L
    private var clockRunning = false
    private var clockStartedAtMs = 0L
    private var slaBreached = false
    private var currentEscalationTier = 0

    private var isPaused = false
    private var pausedAtMs = 0L
    private var totalPausedMs = 0L
    private var pausedTotalNeedsSync = false
    private var lastResumeActorId = ""

    override fun execute(input: TicketWorkflowInput) {
        ticketId = input.ticketId
        ticketNumber = input.ticketNumber
        srmUserId = input.srmUserId
        remainingSlaMs = Duration.ofHours(input.slaHoursApplied).toMillis()

        log.info("Started ticketNum={} with SLA={}h tiers={}", ticketNumber, input.slaHoursApplied, input.escalationTierHoursList)

        // 1. Count down the SLA, stopping the clock while paused.
        while (!clockStopped() && !slaBreached) {
            if (isPaused) {
                Workflow.await { !isPaused || clockStopped() }
            } else {
                clockStartedAtMs = Workflow.currentTimeMillis()
                clockRunning = true
                val wokenEarly = Workflow.await(Duration.ofMillis(remainingSlaMs)) { isPaused || clockStopped() }
                clockRunning = false

                val elapsedMs = (Workflow.currentTimeMillis() - clockStartedAtMs).coerceAtLeast(0L)
                remainingSlaMs = (remainingSlaMs - elapsedMs).coerceAtLeast(0L)
                if (!wokenEarly) {
                    remainingSlaMs = 0L
                    slaBreached = true
                }
            }
            syncPausedTotalIfNeeded()
        }

        // 2. Breach: alert the SRM (tier 0), then climb the management tiers.
        if (slaBreached && !clockStopped()) {
            log.info("SLA breached for ticketNum={}", ticketNumber)
            Workflow.upsertTypedSearchAttributes(ATTR_SLA_STATUS.valueSet("PAST_SLA"))
            escalate(0)

            for ((index, gapHours) in input.escalationTierHoursList.withIndex()) {
                val resolvedInTime = Workflow.await(Duration.ofHours(gapHours)) { clockStopped() }
                if (resolvedInTime) break

                currentEscalationTier = index + 1
                Workflow.upsertTypedSearchAttributes(ATTR_ESCALATION_TIER.valueSet(currentEscalationTier.toLong()))
                escalate(currentEscalationTier)
            }
        }

        // 3. Stay alive (answerable to queries) until the ticket is closed or declined.
        Workflow.await { currentStatus in ENDS_THE_WORKFLOW }
        syncPausedTotalIfNeeded()
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
        log.info("SLA resumed for ticketNum={}; totalPausedSecs={}", ticketNumber, totalPausedMs / 1000)
    }

    override fun onStatusTransition(signal: StatusTransitionSignal) {
        if (isPaused && signal.newStatus in STOPS_THE_CLOCK) {
            closeOpenPause(signal.actorId)
        }
        currentStatus = signal.newStatus
        Workflow.upsertTypedSearchAttributes(ATTR_TICKET_STATUS.valueSet(currentStatus))
    }

    override fun getWorkflowDiagnostics(): TicketWorkflowDiagnostics {
        val now = Workflow.currentTimeMillis()
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
            .setCurrentEscalationTier(currentEscalationTier)
            .build()
    }

    // --- helpers ---

    private fun clockStopped() = currentStatus in STOPS_THE_CLOCK

    private fun closeOpenPause(actorId: String) {
        totalPausedMs += (Workflow.currentTimeMillis() - pausedAtMs).coerceAtLeast(0L)
        isPaused = false
        lastResumeActorId = actorId
        pausedTotalNeedsSync = true
    }

    private fun syncPausedTotalIfNeeded() {
        if (!pausedTotalNeedsSync) return
        pausedTotalNeedsSync = false
        activities.syncPausedDurationToBackend(ticketId, totalPausedMs / 1000, lastResumeActorId)
    }

    private fun escalate(tier: Int) {
        activities.recordAndNotifySlaEscalation(
            ticketId, ticketNumber, tier, srmUserId, Workflow.currentTimeMillis(), totalPausedMs / 1000
        )
    }
}
