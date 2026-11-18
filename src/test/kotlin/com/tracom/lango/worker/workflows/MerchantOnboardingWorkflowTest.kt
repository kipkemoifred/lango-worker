package com.tracom.lango.worker.workflows

import com.tracom.lango.proto.v1.AmendmentResubmittedSignal
import com.tracom.lango.proto.v1.LinkedTicketSignal
import com.tracom.lango.proto.v1.OnboardingStageDecisionSignal
import com.tracom.lango.proto.v1.OnboardingWorkflowDiagnostics
import com.tracom.lango.proto.v1.ProvisionMerchantResponse
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
 * started with a plain map, exactly like RecruitMerchant does from Go.
 * Activities are replaced by [RecordingOnboardingActivities], so no backend is needed.
 *
 * Test policy: review 24h, compliance 48h, ICT 12h, then tiers 4h and 8h apart;
 * 72h to resubmit an amendment; at most 2 returns.
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
    fun `application approved at every stage is provisioned and activated`() {
        start("a1")
        decide("a1", "AWAITING_REVIEW", "APPROVE", "supervisor-1")
        decide("a1", "COMPLIANCE_REVIEW", "APPROVE", "compliance-1")
        decide("a1", "ICT_PROVISIONING", "APPROVE", "ict-1")

        awaitEnd("a1")
        assertEquals(listOf(Provision("a1", "ticket-a1", "ict-1")), activities.provisions)
        assertTrue(activities.overdueAlerts.isEmpty(), "no alerts when every stage is on time")
        assertTrue(activities.declines.isEmpty())

        val d = diagnostics("a1")
        assertEquals("ACTIVATED", d.currentStage)
        assertEquals("merchant-a1", d.merchantId)
        assertEquals("MID-a1", d.mid)
    }

    @Test
    fun `overdue stage alerts its owners then climbs each management tier`() {
        val t0 = start("a2")

        testEnv.sleep(Duration.ofHours(23))
        assertTrue(activities.overdueAlerts.isEmpty(), "nothing before the 24h review deadline")

        testEnv.sleep(Duration.ofHours(17)) // 40h after submission
        eventually { assertEquals(listOf(0, 1, 2), activities.overdueAlerts.map { it.tier }) }

        val alerts = activities.overdueAlerts
        assertTrue(alerts.all { it.stage == "AWAITING_REVIEW" && it.applicationId == "a2" })
        assertAbout(t0 + 24 * HOUR_MS, alerts[0].atMs, "tier 0 (supervisors) at the deadline")
        assertAbout(t0 + 28 * HOUR_MS, alerts[1].atMs, "tier 1 four hours later")
        assertAbout(t0 + 36 * HOUR_MS, alerts[2].atMs, "tier 2 eight hours after that")

        // A late approval still moves on, and the next stage starts its own clean deadline
        decide("a2", "AWAITING_REVIEW", "APPROVE", "supervisor-1")
        eventually {
            assertEquals("COMPLIANCE_REVIEW", diagnostics("a2").currentStage)
            assertEquals(-1, diagnostics("a2").stageEscalationTier)
        }
        testEnv.sleep(Duration.ofHours(47))
        assertEquals(3, activities.overdueAlerts.size, "compliance has 48h before its first alert")
    }

    @Test
    fun `open linked ticket pauses the stage deadline`() {
        start("a3")
        decide("a3", "AWAITING_REVIEW", "APPROVE", "supervisor-1")
        eventually { assertEquals("COMPLIANCE_REVIEW", diagnostics("a3").currentStage) }

        testEnv.sleep(Duration.ofHours(10))
        linked("a3", "inspection-1", opened = true)
        eventually { assertTrue(diagnostics("a3").isStageClockPaused) }

        testEnv.sleep(Duration.ofHours(60)) // far past 48h, but the clock is stopped
        assertTrue(activities.overdueAlerts.isEmpty(), "no alert while the inspection ticket is open")
        assertEquals(listOf("inspection-1"), diagnostics("a3").openLinkedTicketIdsList)

        linked("a3", "inspection-1", opened = false)
        testEnv.sleep(Duration.ofHours(37)) // 47h of compliance clock used
        assertTrue(activities.overdueAlerts.isEmpty())

        testEnv.sleep(Duration.ofHours(2))
        eventually { assertEquals(listOf("COMPLIANCE_REVIEW"), activities.overdueAlerts.map { it.stage }) }
    }

    @Test
    fun `returned application goes back to review when the agent resubmits`() {
        start("a4")
        decide("a4", "AWAITING_REVIEW", "RETURN", "supervisor-1")
        eventually { assertEquals("RETURNED_FOR_AMENDMENT", diagnostics("a4").currentStage) }

        resubmit("a4")
        eventually {
            val d = diagnostics("a4")
            assertEquals("AWAITING_REVIEW", d.currentStage)
            assertEquals(1, d.amendmentCycleCount)
        }

        // Compliance can return it too
        decide("a4", "AWAITING_REVIEW", "APPROVE", "supervisor-1")
        decide("a4", "COMPLIANCE_REVIEW", "RETURN", "compliance-1")
        eventually {
            assertEquals("RETURNED_FOR_AMENDMENT", diagnostics("a4").currentStage)
            assertEquals(2, diagnostics("a4").amendmentCycleCount)
        }
        assertTrue(activities.declines.isEmpty())
    }

    @Test
    fun `agent who does not resubmit in time gets the application declined`() {
        start("a5")
        decide("a5", "AWAITING_REVIEW", "RETURN", "supervisor-1")

        testEnv.sleep(Duration.ofHours(71))
        assertTrue(activities.declines.isEmpty(), "still inside the 72h amendment window")

        testEnv.sleep(Duration.ofHours(2))
        awaitEnd("a5")
        assertEquals(listOf(Decline("a5", "RETURNED_FOR_AMENDMENT", "AMENDMENT_EXPIRED")), activities.declines)
        assertEquals("DECLINED", diagnostics("a5").currentStage)
        assertEquals("AMENDMENT_EXPIRED", diagnostics("a5").declineReason)
    }

    @Test
    fun `one return too many declines the application`() {
        start("a6")
        repeat(2) {
            decide("a6", "AWAITING_REVIEW", "RETURN", "supervisor-1")
            eventually { assertEquals("RETURNED_FOR_AMENDMENT", diagnostics("a6").currentStage) }
            resubmit("a6")
            eventually { assertEquals("AWAITING_REVIEW", diagnostics("a6").currentStage) }
        }
        decide("a6", "AWAITING_REVIEW", "RETURN", "supervisor-1") // third return, limit is 2

        awaitEnd("a6")
        assertEquals(listOf(Decline("a6", "RETURNED_FOR_AMENDMENT", "MAX_AMENDMENTS_EXCEEDED")), activities.declines)
    }

    @Test
    fun `reviewer decline ends the workflow without a decline callback`() {
        start("a7")
        decide("a7", "AWAITING_REVIEW", "APPROVE", "supervisor-1")
        decide("a7", "COMPLIANCE_REVIEW", "DECLINE", "compliance-1")

        awaitEnd("a7")
        assertEquals("DECLINED", diagnostics("a7").currentStage)
        assertEquals("DECLINED_BY_REVIEWER", diagnostics("a7").declineReason)
        assertTrue(activities.declines.isEmpty(), "the backend already stored the reviewer's decline")
        assertTrue(activities.provisions.isEmpty())
    }

    @Test
    fun `stale and invalid signals are ignored`() {
        start("a8")
        decide("a8", "AWAITING_REVIEW", "APPROVE", "supervisor-1")
        decide("a8", "AWAITING_REVIEW", "APPROVE", "supervisor-2") // double click on an old stage
        resubmit("a8")                                              // nothing was returned
        decide("a8", "COMPLIANCE_REVIEW", "MAYBE", "compliance-1") // unknown outcome
        eventually { assertEquals("COMPLIANCE_REVIEW", diagnostics("a8").currentStage) }

        decide("a8", "COMPLIANCE_REVIEW", "APPROVE", "compliance-1")
        decide("a8", "ICT_PROVISIONING", "RETURN", "ict-1")         // ICT cannot return
        testEnv.sleep(Duration.ofMinutes(1))
        eventually { assertEquals("ICT_PROVISIONING", diagnostics("a8").currentStage) }
        assertEquals(0, diagnostics("a8").amendmentCycleCount)
        assertTrue(activities.provisions.isEmpty())
    }

    // --- helpers ---

    /** Starts the workflow exactly as the Go backend does: untyped, with a JSON map. Returns the start time (ms). */
    private fun start(id: String): Long {
        val stub = client.newUntypedWorkflowStub(
            "MerchantOnboardingWorkflow",
            WorkflowOptions.newBuilder()
                .setWorkflowId("onboarding-$id")
                .setTaskQueue(MerchantOnboardingWorkflowImpl.TASK_QUEUE)
                .build()
        )
        val startedAt = testEnv.currentTimeMillis()
        stub.start(
            mapOf(
                "applicationId" to id,
                "ticketId" to "ticket-$id",
                "ticketNumber" to "RS-$id",
                "recruitingAgentId" to "agent-1",
                "branchCode" to "001",
                "regionCode" to "NRB",
            )
        )
        return startedAt
    }

    private fun application(id: String): MerchantOnboardingWorkflow =
        client.newWorkflowStub(MerchantOnboardingWorkflow::class.java, "onboarding-$id")

    private fun decide(id: String, stage: String, outcome: String, actorId: String) =
        application(id).onStageDecision(
            OnboardingStageDecisionSignal.newBuilder().setStage(stage).setOutcome(outcome).setActorId(actorId).build()
        )

    private fun resubmit(id: String) =
        application(id).onAmendmentResubmitted(AmendmentResubmittedSignal.newBuilder().setActorId("agent-1").build())

    private fun linked(id: String, ticketId: String, opened: Boolean) {
        val signal = LinkedTicketSignal.newBuilder()
            .setTicketId(ticketId).setLinkRelationship("SITE_INSPECTION").setActorId("compliance-1").build()
        if (opened) application(id).onLinkedTicketOpened(signal) else application(id).onLinkedTicketResolved(signal)
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
data class Provision(val applicationId: String, val ticketId: String, val actorId: String)

/**
 * Fake onboarding activities: record what the workflow asked for instead of calling the Go backend.
 * Parked on its own worker for the same reason as [RecordingActivities].
 */
@TemporalActivity(workers = ["unit-test-fakes"])
class RecordingOnboardingActivities(
    private val policy: OnboardingPolicy = OnboardingPolicy(24, 48, 12, 72, listOf(4L, 8L), 2)
) : OnboardingActivities {

    val overdueAlerts = CopyOnWriteArrayList<OverdueAlert>()
    val declines = CopyOnWriteArrayList<Decline>()
    val provisions = CopyOnWriteArrayList<Provision>()

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

    override fun provisionMerchant(applicationId: String, ticketId: String, approvedByActorId: String): ProvisionMerchantResponse {
        provisions += Provision(applicationId, ticketId, approvedByActorId)
        return ProvisionMerchantResponse.newBuilder()
            .setSuccess(true)
            .setMerchantId("merchant-$applicationId")
            .setMid("MID-$applicationId")
            .build()
    }
}
