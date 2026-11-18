package com.tracom.lango.worker.workflows

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.tracom.lango.proto.v1.AmendmentResubmittedSignal
import com.tracom.lango.proto.v1.LinkedTicketSignal
import com.tracom.lango.proto.v1.OnboardingStageDecisionSignal
import com.tracom.lango.proto.v1.OnboardingWorkflowDiagnostics
import io.temporal.workflow.QueryMethod
import io.temporal.workflow.SignalMethod
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod

/**
 * One instance runs per recruitment application, with workflow id "onboarding-<application UUID>".
 * Names below must match what the Go backend sends.
 */
@WorkflowInterface
interface MerchantOnboardingWorkflow {

    @WorkflowMethod(name = "MerchantOnboardingWorkflow")
    fun execute(input: OnboardingWorkflowInput)

    @SignalMethod(name = "stageDecision")
    fun onStageDecision(signal: OnboardingStageDecisionSignal)

    @SignalMethod(name = "amendmentResubmitted")
    fun onAmendmentResubmitted(signal: AmendmentResubmittedSignal)

    @SignalMethod(name = "linkedTicketOpened")
    fun onLinkedTicketOpened(signal: LinkedTicketSignal)

    @SignalMethod(name = "linkedTicketResolved")
    fun onLinkedTicketResolved(signal: LinkedTicketSignal)

    @QueryMethod(name = "getOnboardingDiagnostics")
    fun getOnboardingDiagnostics(): OnboardingWorkflowDiagnostics
}

/**
 * Plain JSON, not proto: RecruitMerchant (spec section 4.3) starts the workflow with a
 * Go map, which Temporal encodes as json/plain. Field names must match that map.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class OnboardingWorkflowInput {
    var applicationId: String = ""
    var ticketId: String = ""
    var ticketNumber: String = ""
    var recruitingAgentId: String = ""
    var branchCode: String = ""
    var regionCode: String = ""
}
