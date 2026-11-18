package com.tracom.lango.worker.workflows

import com.tracom.lango.proto.v1.AmendmentResubmittedSignal
import com.tracom.lango.proto.v1.LinkedTicketSignal
import com.tracom.lango.proto.v1.OnboardingStageDecisionSignal
import com.tracom.lango.proto.v1.OnboardingWorkflowDiagnostics
import com.tracom.lango.worker.activities.OnboardingActivities
import com.tracom.lango.worker.activities.OnboardingPolicy
import io.quarkiverse.temporal.TemporalWorkflow
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import io.temporal.common.SearchAttributeKey
import io.temporal.workflow.Workflow
import java.time.Duration

/**
 * Follows one recruitment application from AWAITING_REVIEW to ACTIVATED or DECLINED.
 *
 * Bound to the "onboarding" worker, which polls lango-onboarding-tq (see application.properties).
 *
 * The backend stays the state machine enforcer: it checks the reviewer's role, stores the
 * new stage, then signals here. This workflow owns what needs time or retries:
 *  - a deadline per review stage, then reminders up the management tiers. The clock pauses
 *    while a linked ticket (site inspection, clarification) is open, since those have their own SLA;
 *  - the agent's amendment window, and the limit on how often an application can be returned;
 *  - the provisioning call to TMS / Core Banking, retried until it succeeds.
 */
@TemporalWorkflow(workers = ["onboarding"])
class MerchantOnboardingWorkflowImpl : MerchantOnboardingWorkflow {

    companion object {
        const val TASK_QUEUE = "lango-onboarding-tq"

        const val AWAITING_REVIEW = "AWAITING_REVIEW"
        const val COMPLIANCE_REVIEW = "COMPLIANCE_REVIEW"
        const val ICT_PROVISIONING = "ICT_PROVISIONING"
        const val RETURNED_FOR_AMENDMENT = "RETURNED_FOR_AMENDMENT"
        const val ACTIVATED = "ACTIVATED"
        const val DECLINED = "DECLINED"

        val ATTR_ONBOARDING_STAGE: SearchAttributeKey<String> = SearchAttributeKey.forKeyword("LangoOnboardingStage")
        val ATTR_REGION: SearchAttributeKey<String> = SearchAttributeKey.forKeyword("LangoRegion")
        val ATTR_BRANCH: SearchAttributeKey<String> = SearchAttributeKey.forKeyword("LangoBranch")

        private val FINAL_STAGES = setOf(ACTIVATED, DECLINED)
    }

    private val log = Workflow.getLogger(MerchantOnboardingWorkflowImpl::class.java)

    private val activities: OnboardingActivities = Workflow.newActivityStub(
        OnboardingActivities::class.java,
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
    private var input = OnboardingWorkflowInput()
    private lateinit var policy: OnboardingPolicy
    private var stage = AWAITING_REVIEW
    private var amendmentCycles = 0

    private val pendingDecisions = ArrayDeque<OnboardingStageDecisionSignal>()
    private var resubmitted = false
    private val openLinkedTickets = linkedSetOf<String>()

    private var nextAlarmAtMs = 0L
    private var clockPaused = false
    private var stageEscalationTier = -1

    private var merchantId = ""
    private var mid = ""
    private var declineReason = ""

    override fun execute(input: OnboardingWorkflowInput) {
        this.input = input
        policy = activities.loadOnboardingPolicy()

        log.info("Started onboarding application={} ticketNum={}", input.applicationId, input.ticketNumber)
        if (input.regionCode.isNotBlank()) Workflow.upsertTypedSearchAttributes(ATTR_REGION.valueSet(input.regionCode))
        if (input.branchCode.isNotBlank()) Workflow.upsertTypedSearchAttributes(ATTR_BRANCH.valueSet(input.branchCode))
        Workflow.upsertTypedSearchAttributes(ATTR_ONBOARDING_STAGE.valueSet(stage))

        while (stage !in FINAL_STAGES) {
            if (stage == RETURNED_FOR_AMENDMENT) waitForAmendment() else runReviewStage()
        }
        log.info("Finished onboarding application={} stage={}", input.applicationId, stage)
    }

    override fun onStageDecision(signal: OnboardingStageDecisionSignal) {
        pendingDecisions.addLast(signal)
    }

    override fun onAmendmentResubmitted(signal: AmendmentResubmittedSignal) {
        if (stage != RETURNED_FOR_AMENDMENT) {
            log.warn("Ignoring resubmission by {}: application is {}", signal.actorId, stage)
            return
        }
        resubmitted = true
    }

    override fun onLinkedTicketOpened(signal: LinkedTicketSignal) {
        if (stage in FINAL_STAGES) return
        openLinkedTickets += signal.ticketId
        log.info("Linked {} ticket {} to application={}", signal.linkRelationship, signal.ticketId, input.applicationId)
    }

    override fun onLinkedTicketResolved(signal: LinkedTicketSignal) {
        openLinkedTickets -= signal.ticketId
    }

    override fun getOnboardingDiagnostics(): OnboardingWorkflowDiagnostics =
        OnboardingWorkflowDiagnostics.newBuilder()
            .setApplicationId(input.applicationId)
            .setTicketId(input.ticketId)
            .setTicketNumber(input.ticketNumber)
            .setCurrentStage(stage)
            .setAmendmentCycleCount(amendmentCycles)
            .addAllOpenLinkedTicketIds(openLinkedTickets)
            .setIsStageClockPaused(clockPaused)
            .setNextAlarmEpochMs(nextAlarmAtMs)
            .setStageEscalationTier(stageEscalationTier)
            .setMerchantId(merchantId)
            .setMid(mid)
            .setDeclineReason(declineReason)
            .build()

    // --- stages ---

    /** Waits for the reviewer's decision, alerting the stage owners and then management as it overruns. */
    private fun runReviewStage() {
        val stageAtEntry = stage
        stageEscalationTier = -1
        var untilAlarmMs: Long? = hoursToMs(deadlineHoursFor(stageAtEntry))

        while (stage == stageAtEntry) {
            if (pendingDecisions.isNotEmpty()) {
                applyDecision(pendingDecisions.removeFirst())
                continue
            }
            if (openLinkedTickets.isNotEmpty()) {
                clockPaused = true
                Workflow.await { pendingDecisions.isNotEmpty() || openLinkedTickets.isEmpty() }
                clockPaused = false
                continue
            }

            val alarmMs = untilAlarmMs
            if (alarmMs == null) {
                // every tier has been alerted: just wait for the decision
                Workflow.await { pendingDecisions.isNotEmpty() || openLinkedTickets.isNotEmpty() }
                continue
            }

            val startedAtMs = Workflow.currentTimeMillis()
            nextAlarmAtMs = startedAtMs + alarmMs
            val wokenEarly = Workflow.await(Duration.ofMillis(alarmMs)) {
                pendingDecisions.isNotEmpty() || openLinkedTickets.isNotEmpty()
            }
            nextAlarmAtMs = 0L

            if (wokenEarly) {
                untilAlarmMs = (alarmMs - (Workflow.currentTimeMillis() - startedAtMs)).coerceAtLeast(0L)
            } else {
                stageEscalationTier += 1
                activities.notifyStageOverdue(
                    input.applicationId, input.ticketNumber, stageAtEntry, stageEscalationTier,
                    input.branchCode, input.regionCode, Workflow.currentTimeMillis()
                )
                untilAlarmMs = policy.escalationTierHours.getOrNull(stageEscalationTier)?.let { hoursToMs(it) }
            }
        }
        stageEscalationTier = -1
    }

    /** Gives the sales agent the amendment window; no resubmission in time declines the application. */
    private fun waitForAmendment() {
        resubmitted = false
        var remainingMs = hoursToMs(policy.amendmentDeadlineHours)

        while (stage == RETURNED_FOR_AMENDMENT) {
            if (pendingDecisions.isNotEmpty()) {
                applyDecision(pendingDecisions.removeFirst())
                continue
            }
            if (resubmitted) {
                resubmitted = false
                moveTo(AWAITING_REVIEW)
                return
            }

            val startedAtMs = Workflow.currentTimeMillis()
            nextAlarmAtMs = startedAtMs + remainingMs
            val wokenEarly = Workflow.await(Duration.ofMillis(remainingMs)) {
                resubmitted || pendingDecisions.isNotEmpty()
            }
            nextAlarmAtMs = 0L

            if (!wokenEarly) {
                declineByWorker("AMENDMENT_EXPIRED")
                return
            }
            remainingMs = (remainingMs - (Workflow.currentTimeMillis() - startedAtMs)).coerceAtLeast(0L)
        }
    }

    // --- helpers ---

    private fun applyDecision(decision: OnboardingStageDecisionSignal) {
        if (decision.stage != stage) {
            log.warn(
                "Ignoring {} for stage {}: application={} is in {}",
                decision.outcome, decision.stage, input.applicationId, stage
            )
            return
        }
        when (decision.outcome) {
            "APPROVE" -> when (stage) {
                AWAITING_REVIEW -> moveTo(COMPLIANCE_REVIEW)
                COMPLIANCE_REVIEW -> moveTo(ICT_PROVISIONING)
                ICT_PROVISIONING -> provision(decision.actorId)
                else -> log.warn("Ignoring APPROVE in stage {}", stage)
            }
            "RETURN" -> when (stage) {
                AWAITING_REVIEW, COMPLIANCE_REVIEW -> returnForAmendment()
                else -> log.warn("Ignoring RETURN in stage {}", stage)
            }
            "DECLINE" -> {
                // the backend has already stored DECLINED with the reviewer's remarks
                declineReason = "DECLINED_BY_REVIEWER"
                moveTo(DECLINED)
            }
            else -> log.warn("Ignoring unknown outcome {} from {}", decision.outcome, decision.actorId)
        }
    }

    private fun returnForAmendment() {
        amendmentCycles += 1
        moveTo(RETURNED_FOR_AMENDMENT)
        if (amendmentCycles > policy.maxAmendmentCycles) {
            declineByWorker("MAX_AMENDMENTS_EXCEEDED")
        }
    }

    private fun provision(approvedByActorId: String) {
        val provisioned = activities.provisionMerchant(input.applicationId, input.ticketId, approvedByActorId)
        merchantId = provisioned.merchantId
        mid = provisioned.mid
        moveTo(ACTIVATED)
    }

    private fun declineByWorker(reason: String) {
        activities.declineApplication(input.applicationId, stage, reason)
        declineReason = reason
        moveTo(DECLINED)
    }

    private fun moveTo(newStage: String) {
        log.info("Onboarding application={} {} -> {}", input.applicationId, stage, newStage)
        stage = newStage
        Workflow.upsertTypedSearchAttributes(ATTR_ONBOARDING_STAGE.valueSet(stage))
    }

    private fun deadlineHoursFor(reviewStage: String): Long = when (reviewStage) {
        AWAITING_REVIEW -> policy.reviewDeadlineHours
        COMPLIANCE_REVIEW -> policy.complianceDeadlineHours
        else -> policy.ictDeadlineHours
    }

    private fun hoursToMs(hours: Long): Long = Duration.ofHours(hours).toMillis()
}
