package com.tracom.lango.worker.workflows

import com.tracom.lango.proto.v1.PauseSlaSignal
import com.tracom.lango.proto.v1.ResumeSlaSignal
import com.tracom.lango.proto.v1.StatusTransitionSignal
import com.tracom.lango.proto.v1.TicketWorkflowDiagnostics
import com.tracom.lango.proto.v1.TicketWorkflowInput
import io.temporal.workflow.QueryMethod
import io.temporal.workflow.SignalMethod
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod

/**
 * One instance runs per ticket, with workflow id "ticket-<ticket UUID>".
 * Names below must match what the Go backend sends.
 */
@WorkflowInterface
interface TicketLifecycleWorkflow {

    @WorkflowMethod(name = "TicketLifecycleWorkflow")
    fun execute(input: TicketWorkflowInput)

    @SignalMethod(name = "pauseSla")
    fun pauseSla(signal: PauseSlaSignal)

    @SignalMethod(name = "resumeSla")
    fun resumeSla(signal: ResumeSlaSignal)

    @SignalMethod(name = "statusTransition")
    fun onStatusTransition(signal: StatusTransitionSignal)

    @QueryMethod(name = "getWorkflowDiagnostics")
    fun getWorkflowDiagnostics(): TicketWorkflowDiagnostics
}
