package com.tracom.lango.worker.workflows

import com.tracom.lango.proto.v1.VisitCycleDeactivatedSignal
import com.tracom.lango.proto.v1.VisitCycleDiagnostics
import com.tracom.lango.proto.v1.VisitCycleWorkflowInput
import com.tracom.lango.proto.v1.VisitFrequencyChangedSignal
import com.tracom.lango.proto.v1.VisitOutcomeSignal
import com.tracom.lango.worker.activities.VisitActivities
import com.tracom.lango.worker.activities.VisitPolicy
import io.quarkiverse.temporal.TemporalWorkflow
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import io.temporal.common.SearchAttributeKey
import io.temporal.workflow.Workflow
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.ZoneOffset

/**
 * Keeps one merchant outlet's recurring site visits going: creates each visit a planning
 * horizon before its due date (BA DFD-5 5.2, so the officer and merchant can agree a date),
 * reminds before its window closes, marks it MISSED if the window closes with no outcome,
 * then moves on to the next one.
 *
 * Bound to the "visits" worker, which polls lango-visits-tq (see application.properties).
 *
 *  - Visits stay on a fixed schedule: visit n is due on anchor + n * interval, so a late or
 *    missed visit never shifts the ones after it.
 *  - Visits whose whole window is already in the past (anchor in the past, worker outage) are
 *    skipped rather than created and marked missed at once.
 *  - One visit is open at a time: if the horizon is longer than the gap between visits, the
 *    next visit is created as soon as the current one closes.
 *  - Each run handles one visit and then continues-as-new, so the history stays small even
 *    though a cycle runs for as long as the merchant is active.
 */
@TemporalWorkflow(workers = ["visits"])
class VisitCycleWorkflowImpl : VisitCycleWorkflow {

    companion object {
        const val TASK_QUEUE = "lango-visits-tq"

        const val WAITING_TO_SCHEDULE = "WAITING_TO_SCHEDULE"
        const val VISIT_OPEN = "VISIT_OPEN"
        const val ENDED = "ENDED"

        val ATTR_NEXT_VISIT_DUE: SearchAttributeKey<OffsetDateTime> = SearchAttributeKey.forOffsetDateTime("LangoNextVisitDue")
        val ATTR_REGION: SearchAttributeKey<String> = SearchAttributeKey.forKeyword("LangoRegion")
        val ATTR_BRANCH: SearchAttributeKey<String> = SearchAttributeKey.forKeyword("LangoBranch")

        private val CLOSING_OUTCOMES = setOf("COMPLETED", "WAIVED", "CANCELLED")
    }

    private val log = Workflow.getLogger(VisitCycleWorkflowImpl::class.java)

    private val activities: VisitActivities = Workflow.newActivityStub(
        VisitActivities::class.java,
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
    private var input: VisitCycleWorkflowInput = VisitCycleWorkflowInput.getDefaultInstance()
    private lateinit var policy: VisitPolicy
    private lateinit var zone: ZoneId

    private var phase = WAITING_TO_SCHEDULE
    private var dueDate: LocalDate? = null
    private var currentVisitId = ""
    private var outcome: String? = null
    private var deactivated = false
    private var pendingFrequency: VisitFrequencyChangedSignal? = null

    override fun execute(input: VisitCycleWorkflowInput) {
        this.input = input
        policy = activities.loadVisitPolicy()
        zone = ZoneId.of(policy.zoneId())

        if (input.regionCode.isNotBlank()) Workflow.upsertTypedSearchAttributes(ATTR_REGION.valueSet(input.regionCode))
        if (input.branchCode.isNotBlank()) Workflow.upsertTypedSearchAttributes(ATTR_BRANCH.valueSet(input.branchCode))

        // 1. Wait until the next visit whose window is still open is within the planning horizon
        val visitIndex = firstOpenVisitIndex(input.nextVisitIndex)
        val due = dueDateOf(visitIndex)
        dueDate = due
        val scheduleOn = due.minusDays(policy.planningHorizonDays().toLong())
        val windowEndsAtMs = startOfDayMs(due.plusDays(policy.windowDays().toLong()))
        Workflow.upsertTypedSearchAttributes(
            ATTR_NEXT_VISIT_DUE.valueSet(OffsetDateTime.ofInstant(Instant.ofEpochMilli(startOfDayMs(due)), ZoneOffset.UTC))
        )

        phase = WAITING_TO_SCHEDULE
        Workflow.await(untilMs(startOfDayMs(scheduleOn))) { deactivated || pendingFrequency != null }
        if (deactivated) return end()
        if (pendingFrequency != null) return continueWithNext(visitIndex, input.lastVisitDueDate, input.lastVisitOutcome)

        // 2. Create the visit and wait for its outcome until the window closes (it can be done early)
        val windowEndDate = due.plusDays(policy.windowDays() - 1L).toString()
        currentVisitId = activities.scheduleVisit(input.cycleId, input.merchantId, input.outletId, due.toString(), windowEndDate)
        phase = VISIT_OPEN
        log.info("Visit {} open for cycle={} due={} until {}", currentVisitId, input.cycleId, due, windowEndDate)

        val remindAtMs = windowEndsAtMs - Duration.ofDays(policy.reminderDaysBeforeWindowEnd().toLong()).toMillis()
        if (policy.reminderDaysBeforeWindowEnd() > 0 && remindAtMs > Workflow.currentTimeMillis()) {
            val closedEarly = Workflow.await(untilMs(remindAtMs)) { outcome != null || deactivated }
            if (!closedEarly) activities.remindVisitDue(currentVisitId, input.cycleId, windowEndDate)
        }
        val closedInTime = Workflow.await(untilMs(windowEndsAtMs)) { outcome != null || deactivated }
        if (deactivated) return end()

        val result = if (closedInTime) outcome!! else {
            activities.markVisitMissed(currentVisitId, input.cycleId, due.toString())
            "MISSED"
        }
        log.info("Visit {} for cycle={} due={}: {}", currentVisitId, input.cycleId, due, result)

        // 3. Start a fresh run for the next visit
        if (deactivated) return end()
        continueWithNext(visitIndex + 1, due.toString(), result)
    }

    override fun onVisitOutcome(signal: VisitOutcomeSignal) {
        if (phase != VISIT_OPEN || signal.visitId != currentVisitId || signal.outcome !in CLOSING_OUTCOMES) {
            log.warn("Ignoring {} for visit {}: open visit is '{}'", signal.outcome, signal.visitId, currentVisitId)
            return
        }
        outcome = signal.outcome
    }

    override fun onFrequencyChanged(signal: VisitFrequencyChangedSignal) {
        if (signal.intervalDays <= 0) {
            log.warn("Ignoring frequency change to {} days from {}", signal.intervalDays, signal.actorId)
            return
        }
        pendingFrequency = signal
    }

    override fun onCycleDeactivated(signal: VisitCycleDeactivatedSignal) {
        deactivated = true
        log.info("Visit cycle={} deactivated by {}: {}", input.cycleId, signal.actorId, signal.reason)
    }

    override fun getVisitCycleDiagnostics(): VisitCycleDiagnostics =
        VisitCycleDiagnostics.newBuilder()
            .setCycleId(input.cycleId)
            .setMerchantId(input.merchantId)
            .setOutletId(input.outletId)
            .setIntervalDays(input.intervalDays)
            .setAnchorDate(input.anchorDate)
            .setPhase(phase)
            .setNextDueDate(dueDate?.toString() ?: "")
            .setNextScheduleDate(dueDate?.minusDays(policy.planningHorizonDays().toLong())?.toString() ?: "")
            .setCurrentVisitId(currentVisitId)
            .setCurrentWindowEndDate(
                if (phase == VISIT_OPEN) dueDate?.plusDays(policy.windowDays() - 1L)?.toString() ?: "" else ""
            )
            .setLastVisitDueDate(input.lastVisitDueDate)
            .setLastVisitOutcome(input.lastVisitOutcome)
            .setFrequencyChangePending(pendingFrequency != null)
            .build()

    // --- helpers ---

    /**
     * First visit, from [fromIndex] on, whose window hasn't closed yet and that is due after
     * the last visit (after a frequency change the new schedule may land on that same date).
     */
    private fun firstOpenVisitIndex(fromIndex: Int): Int {
        val nowMs = Workflow.currentTimeMillis()
        val lastDue = input.lastVisitDueDate.takeIf { it.isNotBlank() }?.let(LocalDate::parse)
        var index = fromIndex
        while (startOfDayMs(dueDateOf(index).plusDays(policy.windowDays().toLong())) <= nowMs ||
            (lastDue != null && !dueDateOf(index).isAfter(lastDue))
        ) index++
        if (index > fromIndex) log.warn("Cycle={} skipped {} visit(s) whose window had already closed", input.cycleId, index - fromIndex)
        return index
    }

    private fun dueDateOf(index: Int): LocalDate =
        LocalDate.parse(input.anchorDate).plusDays(index.toLong() * input.intervalDays)

    private fun startOfDayMs(date: LocalDate): Long = date.atStartOfDay(zone).toInstant().toEpochMilli()

    private fun untilMs(atMs: Long): Duration = Duration.ofMillis((atMs - Workflow.currentTimeMillis()).coerceAtLeast(0L))

    /** A frequency change restarts the count from its anchor; otherwise carry on with [nextIndex]. */
    private fun continueWithNext(nextIndex: Int, lastDueDate: String, lastOutcome: String) {
        val next = input.toBuilder()
            .setNextVisitIndex(nextIndex)
            .setLastVisitDueDate(lastDueDate)
            .setLastVisitOutcome(lastOutcome)
        pendingFrequency?.let { change ->
            next.setIntervalDays(change.intervalDays).setNextVisitIndex(0)
            if (change.anchorDate.isNotBlank()) next.setAnchorDate(change.anchorDate)
            log.info("Cycle={} frequency changed to every {} days by {}", input.cycleId, change.intervalDays, change.actorId)
        }
        Workflow.continueAsNew(next.build())
    }

    private fun end() {
        phase = ENDED
        log.info("Visit cycle={} ended", input.cycleId)
    }
}
