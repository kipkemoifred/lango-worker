package com.tracom.lango.worker.workflows

import com.tracom.lango.proto.v1.AmendmentResubmittedSignal
import com.tracom.lango.proto.v1.InspectionSubmittedSignal
import com.tracom.lango.proto.v1.OnboardingStageDecisionSignal
import com.tracom.lango.proto.v1.OnboardingWorkflowDiagnostics
import com.tracom.lango.proto.v1.OnboardingWorkflowInput
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

    @SignalMethod(name = "inspectionSubmitted")
    fun onInspectionSubmitted(signal: InspectionSubmittedSignal)

    @QueryMethod(name = "getOnboardingDiagnostics")
    fun getOnboardingDiagnostics(): OnboardingWorkflowDiagnostics
}
