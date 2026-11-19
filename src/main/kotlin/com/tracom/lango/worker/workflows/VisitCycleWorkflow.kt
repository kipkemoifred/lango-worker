package com.tracom.lango.worker.workflows

import com.tracom.lango.proto.v1.VisitCycleDeactivatedSignal
import com.tracom.lango.proto.v1.VisitCycleDiagnostics
import com.tracom.lango.proto.v1.VisitCycleWorkflowInput
import com.tracom.lango.proto.v1.VisitFrequencyChangedSignal
import com.tracom.lango.proto.v1.VisitOutcomeSignal
import io.temporal.workflow.QueryMethod
import io.temporal.workflow.SignalMethod
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod

/**
 * One instance runs per visit cycle, with workflow id "visit-cycle-<cycle UUID>".
 * Names below must match what the Go backend sends.
 */
@WorkflowInterface
interface VisitCycleWorkflow {

    @WorkflowMethod(name = "VisitCycleWorkflow")
    fun execute(input: VisitCycleWorkflowInput)

    @SignalMethod(name = "visitOutcome")
    fun onVisitOutcome(signal: VisitOutcomeSignal)

    @SignalMethod(name = "frequencyChanged")
    fun onFrequencyChanged(signal: VisitFrequencyChangedSignal)

    @SignalMethod(name = "cycleDeactivated")
    fun onCycleDeactivated(signal: VisitCycleDeactivatedSignal)

    @QueryMethod(name = "getVisitCycleDiagnostics")
    fun getVisitCycleDiagnostics(): VisitCycleDiagnostics
}
