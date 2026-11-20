package com.tracom.lango.worker.workflows

import com.tracom.lango.proto.v1.AmendmentResubmittedSignal
import com.tracom.lango.proto.v1.InspectionSubmittedSignal
import com.tracom.lango.proto.v1.OnboardingStageDecisionSignal
import com.tracom.lango.proto.v1.OnboardingWorkflowDiagnostics
import com.tracom.lango.proto.v1.OnboardingWorkflowInput
import com.tracom.lango.worker.activities.OnboardingActivities
import com.tracom.lango.worker.activities.OnboardingPolicy
import io.quarkiverse.temporal.TemporalWorkflow
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import io.temporal.common.SearchAttributeKey
import io.temporal.workflow.Workflow
import java.time.Duration

/**
 * Follows one recruitment application from AWAITING_REVIEW to ACTIVATED or DECLINED, in the
 * order of the BA data flow DFD-3:
 *
 *   AWAITING_REVIEW (3.2, branch supervisor) -> COMPLIANCE_REVIEW (3.3, compliance and risk)
 *   -> SITE_INSPECTION (3.4, sales agent captures it) -> INSPECTION_APPROVAL (3.5, branch
 *   supervisor approves or returns it) -> ICT_PROVISIONING (3.6, ICT administrator supplies
 *   MID and TID) -> ACTIVATED
 *
 * Bound to the "onboarding" worker, which polls lango-onboarding-tq (see application.properties).
 *
 * The backend stays the state machine enforcer: it checks the actor's role, stores the new
 * stage (and, on activation, the MID and TID), then signals here. This workflow owns what
 * needs time: a deadline per stage with reminders up the management tiers, the agent's
 * amendment window, and the limit on how often an application can be returned.
 */
@TemporalWorkflow(workers = ["onboarding"])
class MerchantOnboardingWorkflowImpl : MerchantOnboardingWorkflow {

    companion object {
        const val TASK_QUEUE = "lango-onboarding-tq"

        const val AWAITING_REVIEW = "AWAITING_REVIEW"
        const val COMPLIANCE_REVIEW = "COMPLIANCE_REVIEW"
        const val SITE_INSPECTION = "SITE_INSPECTION"
        const val INSPECTION_APPROVAL = "INSPECTION_APPROVAL"
        const val ICT_PROVISIONING = "ICT_PROVISIONING"
        const val RETURNED_FOR_AMENDMENT = "RETURNED_FOR_AMENDMENT"
        const val ACTIVATED = "ACTIVATED"
        const val DECLINED = "DECLINED"

        val ATTR_ONBOARDING_STAGE: SearchAttributeKey<String> = SearchAttributeKey.forKeyword("LangoOnboardingStage")
        val ATTR_REGION: SearchAttributeKey<String> = SearchAttributeKey.forKeyword("LangoRegion")
        val ATTR_BRANCH: SearchAttributeKey<String> = SearchAttributeKey.forKeyword("LangoBranch")

        // Outcomes for the agent's own signals, queued with the reviewers' decisions (see onStageDecision)
        private const val SUBMIT = "SUBMIT"
        private const val RESUBMIT = "RESUBMIT"

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
    private var input: OnboardingWorkflowInput = OnboardingWorkflowInput.getDefaultInstance()
    private lateinit var policy: OnboardingPolicy
    private var stage = AWAITING_REVIEW
    private var amendmentCycles = 0
    private var inspectionReturns = 0

    private val pendingDecisions = ArrayDeque<OnboardingStageDecisionSignal>()

    private var nextAlarmAtMs = 0L
    private var stageEscalationTier = -1

    private var merchantId = ""
    private var mid = ""
    private var tid = ""
    private var declineReason = ""

    override fun execute(input: OnboardingWorkflowInput) {
        this.input = input
        policy = activities.loadOnboardingPolicy()

        log.info("Started onboarding application={} ticketNum={}", input.applicationId, input.ticketNumber)
        if (input.regionCode.isNotBlank()) Workflow.upsertTypedSearchAttributes(ATTR_REGION.valueSet(input.regionCode))
        if (input.branchCode.isNotBlank()) Workflow.upsertTypedSearchAttributes(ATTR_BRANCH.valueSet(input.branchCode))
        Workflow.upsertTypedSearchAttributes(ATTR_ONBOARDING_STAGE.valueSet(stage))

        while (stage !in FINAL_STAGES) {
            if (stage == RETURNED_FOR_AMENDMENT) waitForAmendment() else runStage()
        }
        log.info("Finished onboarding application={} stage={}", input.applicationId, stage)
    }

    // Every signal goes into one queue and is applied in arrival order by the main loop. Checking
    // the stage here instead would drop a resubmission or inspection that arrives just after the
    // decision that leads to it, before the main loop has applied that decision.
    override fun onStageDecision(signal: OnboardingStageDecisionSignal) {
        pendingDecisions.addLast(signal)
    }

    override fun onAmendmentResubmitted(signal: AmendmentResubmittedSignal) {
        pendingDecisions.addLast(agentEvent(RETURNED_FOR_AMENDMENT, RESUBMIT, signal.actorId))
    }

    override fun onInspectionSubmitted(signal: InspectionSubmittedSignal) {
        pendingDecisions.addLast(agentEvent(SITE_INSPECTION, SUBMIT, signal.actorId))
    }

    override fun getOnboardingDiagnostics(): OnboardingWorkflowDiagnostics =
        OnboardingWorkflowDiagnostics.newBuilder()
            .setApplicationId(input.applicationId)
            .setTicketId(input.ticketId)
            .setTicketNumber(input.ticketNumber)
            .setCurrentStage(stage)
            .setAmendmentCycleCount(amendmentCycles)
            .setInspectionReturnCount(inspectionReturns)
            .setNextAlarmEpochMs(nextAlarmAtMs)
            .setStageEscalationTier(stageEscalationTier)
            .setMerchantId(merchantId)
            .setMid(mid)
            .setTid(tid)
            .setDeclineReason(declineReason)
            .build()

    // --- stages ---

    /**
     * Waits for whoever owns the stage (a reviewer's decision, or for SITE_INSPECTION the agent's
     * submitted inspection), alerting the stage owners and then management as it overruns.
     */
    private fun runStage() {
        val stageAtEntry = stage
        stageEscalationTier = -1
        var untilAlarmMs: Long? = hoursToMs(deadlineHoursFor(stageAtEntry))

        while (stage == stageAtEntry) {
            if (pendingDecisions.isNotEmpty()) {
                applyDecision(pendingDecisions.removeFirst())
                continue
            }

            val alarmMs = untilAlarmMs
            if (alarmMs == null) {
                // every tier has been alerted: just wait for the stage owner
                Workflow.await { pendingDecisions.isNotEmpty() }
                continue
            }

            val startedAtMs = Workflow.currentTimeMillis()
            nextAlarmAtMs = startedAtMs + alarmMs
            val wokenEarly = Workflow.await(Duration.ofMillis(alarmMs)) { pendingDecisions.isNotEmpty() }
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
        var remainingMs = hoursToMs(policy.amendmentDeadlineHours)

        while (stage == RETURNED_FOR_AMENDMENT) {
            if (pendingDecisions.isNotEmpty()) {
                applyDecision(pendingDecisions.removeFirst())
                continue
            }

            val startedAtMs = Workflow.currentTimeMillis()
            nextAlarmAtMs = startedAtMs + remainingMs
            val wokenEarly = Workflow.await(Duration.ofMillis(remainingMs)) { pendingDecisions.isNotEmpty() }
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
                COMPLIANCE_REVIEW -> moveTo(SITE_INSPECTION)
                INSPECTION_APPROVAL -> moveTo(ICT_PROVISIONING)
                ICT_PROVISIONING -> activate(decision)
                else -> log.warn("Ignoring APPROVE in stage {}", stage)
            }
            SUBMIT -> if (stage == SITE_INSPECTION) moveTo(INSPECTION_APPROVAL) else log.warn("Ignoring SUBMIT in stage {}", stage)
            RESUBMIT -> if (stage == RETURNED_FOR_AMENDMENT) moveTo(AWAITING_REVIEW) else log.warn("Ignoring RESUBMIT in stage {}", stage)
            "RETURN" -> when (stage) {
                AWAITING_REVIEW, COMPLIANCE_REVIEW -> returnForAmendment()
                INSPECTION_APPROVAL -> {
                    // the supervisor sends the inspection back to the agent, not the whole application
                    inspectionReturns += 1
                    moveTo(SITE_INSPECTION)
                }
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

    /** ICT has entered the MID and TID and the backend has stored them: nothing is left to call. */
    private fun activate(decision: OnboardingStageDecisionSignal) {
        if (decision.mid.isBlank() || decision.tid.isBlank()) {
            log.warn("Ignoring activation by {} for application={}: MID or TID missing", decision.actorId, input.applicationId)
            return
        }
        merchantId = decision.merchantId
        mid = decision.mid
        tid = decision.tid
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

    private fun agentEvent(forStage: String, outcome: String, actorId: String): OnboardingStageDecisionSignal =
        OnboardingStageDecisionSignal.newBuilder().setStage(forStage).setOutcome(outcome).setActorId(actorId).build()

    private fun deadlineHoursFor(timedStage: String): Long = when (timedStage) {
        AWAITING_REVIEW -> policy.reviewDeadlineHours
        COMPLIANCE_REVIEW -> policy.complianceDeadlineHours
        SITE_INSPECTION -> policy.inspectionDeadlineHours
        INSPECTION_APPROVAL -> policy.inspectionApprovalDeadlineHours
        else -> policy.ictDeadlineHours
    }

    private fun hoursToMs(hours: Long): Long = Duration.ofHours(hours).toMillis()
}
