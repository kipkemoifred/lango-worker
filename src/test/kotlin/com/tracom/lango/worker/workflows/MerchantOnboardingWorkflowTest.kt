package com.tracom.lango.worker.workflows

import com.tracom.lango.proto.v1.AmendmentResubmittedSignal
import com.tracom.lango.proto.v1.InspectionSubmittedSignal
import com.tracom.lango.proto.v1.OnboardingStageDecisionSignal
import com.tracom.lango.proto.v1.OnboardingWorkflowDiagnostics
import com.tracom.lango.proto.v1.OnboardingWorkflowInput
import com.tracom.lango.worker.activities.OnboardingActivities
import com.tracom.lango.worker.activities.OnboardingPolicy
import com.tracom.lango.worker.testsupport.LangoSearchAttributes
import io.quarkiverse.temporal.TemporalActivity
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import io.temporal.client.WorkflowStub
import io.temporal.testing.TestWorkflowEnvironment
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.math.abs

/**
 * Onboarding logic on Temporal's in-memory server with time skipping. Every application is
 * started with the OnboardingWorkflowInput proto message, as RecruitMerchant does from Go.
 * Activities are replaced by [RecordingOnboardingActivities], so no backend is needed.
 *
 * Stages follow the BA data flow DFD-3 (3.2 review, 3.3 compliance, 3.4 inspection capture,
 * 3.5 inspection approval, 3.6 activation).
 *
 * Test policy: review 24h, compliance 48h, inspection 72h, inspection approval 24h, ICT 12h,
 * then tiers 4h and 8h apart; 72h to resubmit an amendment; at most 2 returns.
 */
@Timeout(60)
class MerchantOnboardingWorkflowTest {

    private lateinit var testEnv: TestWorkflowEnvironment
    private lateinit var client: WorkflowClient
    private lateinit var activities: RecordingOnboardingActivities

    @BeforeEach
    fun setUp() {
        testEnv = TestWorkflowEnvironment.newInstance()
        LangoSearchAttributes.register(testEnv)

        val worker = testEnv.newWorker(MerchantOnboardingWorkflowImpl.TASK_QUEUE)
        worker.registerWorkflowImplementationTypes(MerchantOnboardingWorkflowImpl::class.java)
        activities = RecordingOnboardingActivities()
        worker.registerActivitiesImplementations(activities)

        testEnv.start()
        client = testEnv.workflowClient
    }

    @AfterEach
    fun tearDown() {
        testEnv.close()
    }

    @Test
    fun `application goes through every BA stage and is activated with ICT's MID and TID`() {
        start("a1")
        decide("a1", "AWAITING_REVIEW", "APPROVE", "supervisor-1")
        decide("a1", "COMPLIANCE_REVIEW", "APPROVE", "compliance-1")
        eventually { assertEquals("SITE_INSPECTION", diagnostics("a1").currentStage) }

        inspect("a1")
        eventually { assertEquals("INSPECTION_APPROVAL", diagnostics("a1").currentStage) }
        decide("a1", "INSPECTION_APPROVAL", "APPROVE", "supervisor-1")
        eventually { assertEquals("ICT_PROVISIONING", diagnostics("a1").currentStage) }
        activate("a1")

        awaitEnd("a1")
        val d = diagnostics("a1")
        assertEquals("ACTIVATED", d.currentStage)
        assertEquals("merchant-a1", d.merchantId)
        assertEquals("MID-a1", d.mid)
        assertEquals("TID-a1", d.tid)
        assertTrue(activities.overdueAlerts.isEmpty(), "no alerts when every stage is on time")
        assertTrue(activities.declines.isEmpty())
    }

    @Test
    fun `signals sent back to back are applied in order`() {
        // the backend may send the agent's inspection right after compliance approves:
        // it must not be dropped because the approval hadn't been applied yet
        start("a2")
        decide("a2", "AWAITING_REVIEW", "APPROVE", "supervisor-1")
        decide("a2", "COMPLIANCE_REVIEW", "APPROVE", "compliance-1")
        inspect("a2")
        decide("a2", "INSPECTION_APPROVAL", "APPROVE", "supervisor-1")
        activate("a2")
        awaitEnd("a2")
        assertEquals("ACTIVATED", diagnostics("a2").currentStage)

        start("a3")
        decide("a3", "AWAITING_REVIEW", "RETURN", "supervisor-1")
        resubmit("a3")
        eventually {
            val d = diagnostics("a3")
            assertEquals("AWAITING_REVIEW", d.currentStage)
            assertEquals(1, d.amendmentCycleCount)
        }
    }

    @Test
    fun `overdue stage alerts its owners then climbs each management tier`() {
        val t0 = start("a4")

        testEnv.sleep(Duration.ofHours(23))
        assertTrue(activities.overdueAlerts.isEmpty(), "nothing before the 24h review deadline")

        testEnv.sleep(Duration.ofHours(17)) // 40h after submission
        eventually { assertEquals(listOf(0, 1, 2), activities.overdueAlerts.map { it.tier }) }

        val alerts = activities.overdueAlerts
        assertTrue(alerts.all { it.stage == "AWAITING_REVIEW" && it.applicationId == "a4" })
        assertAbout(t0 + 24 * HOUR_MS, alerts[0].atMs, "tier 0 (supervisors) at the deadline")
        assertAbout(t0 + 28 * HOUR_MS, alerts[1].atMs, "tier 1 four hours later")
        assertAbout(t0 + 36 * HOUR_MS, alerts[2].atMs, "tier 2 eight hours after that")

        // A late approval still moves on, and the next stage starts its own clean deadline
        decide("a4", "AWAITING_REVIEW", "APPROVE", "supervisor-1")
        eventually {
            assertEquals("COMPLIANCE_REVIEW", diagnostics("a4").currentStage)
            assertEquals(-1, diagnostics("a4").stageEscalationTier)
        }
        testEnv.sleep(Duration.ofHours(47))
        assertEquals(3, activities.overdueAlerts.size, "compliance has 48h before its first alert")
    }

    @Test
    fun `site inspection and its approval have their own deadlines`() {
        start("a5")
        decide("a5", "AWAITING_REVIEW", "APPROVE", "supervisor-1")
        decide("a5", "COMPLIANCE_REVIEW", "APPROVE", "compliance-1")
        eventually { assertEquals("SITE_INSPECTION", diagnostics("a5").currentStage) }

        testEnv.sleep(Duration.ofHours(71))
        assertTrue(activities.overdueAlerts.isEmpty(), "the agent has 72h to capture the inspection")
        testEnv.sleep(Duration.ofHours(2))
        eventually { assertEquals(listOf("SITE_INSPECTION"), activities.overdueAlerts.map { it.stage }) }

        inspect("a5")
        eventually { assertEquals("INSPECTION_APPROVAL", diagnostics("a5").currentStage) }
        testEnv.sleep(Duration.ofHours(25))
        eventually { assertEquals(listOf("SITE_INSPECTION", "INSPECTION_APPROVAL"), activities.overdueAlerts.map { it.stage }) }
    }

    @Test
    fun `supervisor can return the inspection to the agent without returning the application`() {
        start("a6")
        decide("a6", "AWAITING_REVIEW", "APPROVE", "supervisor-1")
        decide("a6", "COMPLIANCE_REVIEW", "APPROVE", "compliance-1")
        inspect("a6")
        decide("a6", "INSPECTION_APPROVAL", "RETURN", "supervisor-1")

        eventually {
            val d = diagnostics("a6")
            assertEquals("SITE_INSPECTION", d.currentStage)
            assertEquals(1, d.inspectionReturnCount)
            assertEquals(0, d.amendmentCycleCount, "an inspection return is not an amendment")
        }

        inspect("a6")
        eventually { assertEquals("INSPECTION_APPROVAL", diagnostics("a6").currentStage) }
    }

    @Test
    fun `ICT approval without a MID or TID is ignored`() {
        start("a7")
        toIct("a7")

        decide("a7", "ICT_PROVISIONING", "APPROVE", "ict-1") // no MID or TID
        testEnv.sleep(Duration.ofMinutes(1))
        eventually { assertEquals("ICT_PROVISIONING", diagnostics("a7").currentStage) }

        activate("a7")
        awaitEnd("a7")
        assertEquals("MID-a7", diagnostics("a7").mid)
    }

    @Test
    fun `returned application goes back to review when the agent resubmits`() {
        start("a8")
        decide("a8", "AWAITING_REVIEW", "RETURN", "supervisor-1")
        eventually { assertEquals("RETURNED_FOR_AMENDMENT", diagnostics("a8").currentStage) }

        resubmit("a8")
        eventually {
            val d = diagnostics("a8")
            assertEquals("AWAITING_REVIEW", d.currentStage)
            assertEquals(1, d.amendmentCycleCount)
        }

        // Compliance can return it too (BA to confirm, see the handoff doc)
        decide("a8", "AWAITING_REVIEW", "APPROVE", "supervisor-1")
        decide("a8", "COMPLIANCE_REVIEW", "RETURN", "compliance-1")
        eventually {
            assertEquals("RETURNED_FOR_AMENDMENT", diagnostics("a8").currentStage)
            assertEquals(2, diagnostics("a8").amendmentCycleCount)
        }
        assertTrue(activities.declines.isEmpty())
    }

    @Test
    fun `agent who does not resubmit in time gets the application declined`() {
        start("a9")
        decide("a9", "AWAITING_REVIEW", "RETURN", "supervisor-1")

        testEnv.sleep(Duration.ofHours(71))
        assertTrue(activities.declines.isEmpty(), "still inside the 72h amendment window")

        testEnv.sleep(Duration.ofHours(2))
        awaitEnd("a9")
        assertEquals(listOf(Decline("a9", "RETURNED_FOR_AMENDMENT", "AMENDMENT_EXPIRED")), activities.declines)
        assertEquals("DECLINED", diagnostics("a9").currentStage)
        assertEquals("AMENDMENT_EXPIRED", diagnostics("a9").declineReason)
    }

    @Test
    fun `one return too many declines the application`() {
        start("a10")
        repeat(2) {
            decide("a10", "AWAITING_REVIEW", "RETURN", "supervisor-1")
            resubmit("a10")
        }
        decide("a10", "AWAITING_REVIEW", "RETURN", "supervisor-1") // third return, limit is 2

        awaitEnd("a10")
        assertEquals(listOf(Decline("a10", "RETURNED_FOR_AMENDMENT", "MAX_AMENDMENTS_EXCEEDED")), activities.declines)
    }

    @Test
    fun `reviewer decline ends the workflow without a decline callback`() {
        start("a11")
        decide("a11", "AWAITING_REVIEW", "APPROVE", "supervisor-1")
        decide("a11", "COMPLIANCE_REVIEW", "DECLINE", "compliance-1")

        awaitEnd("a11")
        assertEquals("DECLINED", diagnostics("a11").currentStage)
        assertEquals("DECLINED_BY_REVIEWER", diagnostics("a11").declineReason)
        assertTrue(activities.declines.isEmpty(), "the backend already stored the reviewer's decline")
    }

    @Test
    fun `stale and invalid signals are ignored`() {
        start("a12")
        inspect("a12")                                                // no inspection is due yet
        resubmit("a12")                                               // nothing was returned
        decide("a12", "AWAITING_REVIEW", "SUBMIT", "supervisor-1")   // an agent outcome sent as a decision
        decide("a12", "AWAITING_REVIEW", "APPROVE", "supervisor-1")
        decide("a12", "AWAITING_REVIEW", "APPROVE", "supervisor-2")  // double click on an old stage
        decide("a12", "COMPLIANCE_REVIEW", "MAYBE", "compliance-1")  // unknown outcome
        eventually { assertEquals("COMPLIANCE_REVIEW", diagnostics("a12").currentStage) }

        decide("a12", "COMPLIANCE_REVIEW", "APPROVE", "compliance-1")
        inspect("a12")
        decide("a12", "INSPECTION_APPROVAL", "APPROVE", "supervisor-1")
        decide("a12", "ICT_PROVISIONING", "RETURN", "ict-1")          // ICT cannot return
        testEnv.sleep(Duration.ofMinutes(1))
        eventually { assertEquals("ICT_PROVISIONING", diagnostics("a12").currentStage) }
        assertEquals(0, diagnostics("a12").amendmentCycleCount)
    }

    // --- helpers ---

    /** Starts the workflow the way RecruitMerchant does. Returns the start time (ms). */
    private fun start(id: String): Long {
        val stub = client.newWorkflowStub(
            MerchantOnboardingWorkflow::class.java,
            WorkflowOptions.newBuilder()
                .setWorkflowId("onboarding-$id")
                .setTaskQueue(MerchantOnboardingWorkflowImpl.TASK_QUEUE)
                .build()
        )
        val startedAt = testEnv.currentTimeMillis()
        WorkflowStub.fromTyped(stub).start(
            OnboardingWorkflowInput.newBuilder()
                .setApplicationId(id)
                .setTicketId("ticket-$id")
                .setTicketNumber("RS-$id")
                .setRecruitingAgentId("agent-1")
                .setBranchCode("001")
                .setRegionCode("NRB")
                .build()
        )
        return startedAt
    }

    private fun application(id: String): MerchantOnboardingWorkflow =
        client.newWorkflowStub(MerchantOnboardingWorkflow::class.java, "onboarding-$id")

    private fun decide(id: String, stage: String, outcome: String, actorId: String) =
        application(id).onStageDecision(
            OnboardingStageDecisionSignal.newBuilder().setStage(stage).setOutcome(outcome).setActorId(actorId).build()
        )

    /** The ICT administrator approves with the MID and TID the backend has just stored. */
    private fun activate(id: String) =
        application(id).onStageDecision(
            OnboardingStageDecisionSignal.newBuilder()
                .setStage("ICT_PROVISIONING").setOutcome("APPROVE").setActorId("ict-1")
                .setMerchantId("merchant-$id").setOutletId("outlet-$id").setMid("MID-$id").setTid("TID-$id")
                .build()
        )

    private fun inspect(id: String) =
        application(id).onInspectionSubmitted(
            InspectionSubmittedSignal.newBuilder().setActorId("agent-1").setInspectionId("inspection-$id").build()
        )

    private fun resubmit(id: String) =
        application(id).onAmendmentResubmitted(AmendmentResubmittedSignal.newBuilder().setActorId("agent-1").build())

    private fun toIct(id: String) {
        decide(id, "AWAITING_REVIEW", "APPROVE", "supervisor-1")
        decide(id, "COMPLIANCE_REVIEW", "APPROVE", "compliance-1")
        inspect(id)
        decide(id, "INSPECTION_APPROVAL", "APPROVE", "supervisor-1")
        eventually { assertEquals("ICT_PROVISIONING", diagnostics(id).currentStage) }
    }

    private fun diagnostics(id: String): OnboardingWorkflowDiagnostics = application(id).getOnboardingDiagnostics()

    private fun awaitEnd(id: String) {
        WorkflowStub.fromTyped(application(id)).getResult(Void::class.java)
    }

    /** Signals and activities are processed asynchronously, so give them a moment. */
    private fun eventually(assertion: () -> Unit) {
        val deadline = System.currentTimeMillis() + 5_000
        while (true) {
            try {
                assertion(); return
            } catch (e: AssertionError) {
                if (System.currentTimeMillis() > deadline) throw e
                Thread.sleep(50)
            }
        }
    }

    /** Test-server time also drifts with the wall clock, so allow a minute of slack. */
    private fun assertAbout(expected: Long, actual: Long, what: String) {
        assertTrue(abs(actual - expected) <= 60_000L, "$what: expected ~$expected but was $actual")
    }

    companion object {
        private const val HOUR_MS = 3_600_000L
    }
}

data class OverdueAlert(val applicationId: String, val stage: String, val tier: Int, val atMs: Long)
data class Decline(val applicationId: String, val stage: String, val reason: String)

/**
 * Fake onboarding activities: record what the workflow asked for instead of calling the Go backend.
 * Parked on its own worker for the same reason as [RecordingActivities].
 */
@TemporalActivity(workers = ["unit-test-fakes"])
class RecordingOnboardingActivities(
    private val policy: OnboardingPolicy = OnboardingPolicy(24, 48, 72, 24, 12, 72, listOf(4L, 8L), 2)
) : OnboardingActivities {

    val overdueAlerts = CopyOnWriteArrayList<OverdueAlert>()
    val declines = CopyOnWriteArrayList<Decline>()

    override fun loadOnboardingPolicy(): OnboardingPolicy = policy

    override fun notifyStageOverdue(
        applicationId: String, ticketNumber: String, stage: String, escalationTier: Int,
        branchCode: String, regionCode: String, overdueAtEpochMs: Long
    ) {
        overdueAlerts += OverdueAlert(applicationId, stage, escalationTier, overdueAtEpochMs)
    }

    override fun declineApplication(applicationId: String, stage: String, reason: String) {
        declines += Decline(applicationId, stage, reason)
    }
}
