package com.tracom.lango.worker.workflows

import com.tracom.lango.proto.v1.PauseSlaSignal
import com.tracom.lango.proto.v1.ResumeSlaSignal
import com.tracom.lango.proto.v1.StatusTransitionSignal
import com.tracom.lango.proto.v1.TicketWorkflowInput
import com.tracom.lango.worker.activities.TicketActivities
import com.tracom.lango.worker.testsupport.LangoSearchAttributes
import io.quarkiverse.temporal.TemporalActivity
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import io.temporal.client.WorkflowStub
import io.temporal.testing.TestWorkflowEnvironment
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.math.abs

/**
 * Workflow logic tests on Temporal's in-memory server with time skipping:
 * days of SLA time pass in milliseconds, so no test waits for real.
 * Activities are replaced by [RecordingActivities], so no backend is needed.
 */
@Timeout(60)
class TicketLifecycleWorkflowTest {

    private lateinit var testEnv: TestWorkflowEnvironment
    private lateinit var client: WorkflowClient
    private lateinit var activities: RecordingActivities

    @BeforeEach
    fun setUp() {
        testEnv = TestWorkflowEnvironment.newInstance()
        LangoSearchAttributes.register(testEnv)

        val worker = testEnv.newWorker(TicketLifecycleWorkflowImpl.TASK_QUEUE)
        worker.registerWorkflowImplementationTypes(TicketLifecycleWorkflowImpl::class.java)
        activities = RecordingActivities()
        worker.registerActivitiesImplementations(activities)

        testEnv.start()
        client = testEnv.workflowClient
    }

    @AfterEach
    fun tearDown() {
        testEnv.close()
    }

    @Test
    fun `ticket resolved before the deadline is never escalated`() {
        startTicket("t1", slaHours = 24, tiers = listOf(4, 8))

        testEnv.sleep(Duration.ofHours(10))
        setStatus("t1", "RESOLVED")
        testEnv.sleep(Duration.ofHours(48))

        assertTrue(activities.escalations.isEmpty(), "no escalations expected")
        val d = ticket("t1").getWorkflowDiagnostics()
        assertEquals("RESOLVED", d.currentStatus)
        assertEquals("WITHIN_SLA", d.slaStatus)
        assertAbout(14 * HOUR_S, d.remainingSlaSeconds, "remaining SLA frozen at resolution")

        closeAndAwait("t1")
    }

    @Test
    fun `breach alerts the SRM then climbs each management tier`() {
        val t0 = startTicket("t2", slaHours = 24, tiers = listOf(4, 8))

        testEnv.sleep(Duration.ofHours(23))
        assertTrue(activities.escalations.isEmpty(), "nothing before the 24h deadline")

        testEnv.sleep(Duration.ofHours(17)) // now 40h after creation
        eventually { assertEquals(listOf(0, 1, 2), activities.escalations.map { it.tier }) }

        val e = activities.escalations
        assertAbout(t0 + 24 * HOUR_MS, e[0].atMs, "tier 0 (SRM) at the deadline")
        assertAbout(t0 + 28 * HOUR_MS, e[1].atMs, "tier 1 four hours later")
        assertAbout(t0 + 36 * HOUR_MS, e[2].atMs, "tier 2 eight hours after that")
        assertTrue(e.all { it.srmUserId == "srm-1" && it.ticketNumber == "RS-t2" })

        val d = ticket("t2").getWorkflowDiagnostics()
        assertEquals("PAST_SLA", d.slaStatus)
        assertEquals(2, d.currentEscalationTier)
        assertEquals(0L, d.remainingSlaSeconds)

        closeAndAwait("t2")
    }

    @Test
    fun `pausing stops the clock and pushes the deadline back`() {
        val t0 = startTicket("t3", slaHours = 24)

        testEnv.sleep(Duration.ofHours(6))
        ticket("t3").pauseSla(PauseSlaSignal.newBuilder().setActorId("officer-1").setReason("waiting on merchant").build())

        testEnv.sleep(Duration.ofHours(3))
        val paused = ticket("t3").getWorkflowDiagnostics()
        assertTrue(paused.isSlaPaused)
        assertAbout(18 * HOUR_S, paused.remainingSlaSeconds, "clock frozen at 18h left")

        ticket("t3").resumeSla(ResumeSlaSignal.newBuilder().setActorId("officer-1").build())
        eventually { assertEquals(1, activities.pauseSyncs.size) }
        assertAbout(3 * HOUR_S, activities.pauseSyncs[0].totalPausedSeconds, "3h of pause reported to backend")
        assertEquals("officer-1", activities.pauseSyncs[0].actorId)

        testEnv.sleep(Duration.ofHours(17)) // 26h after creation: old deadline passed, new one (27h) not yet
        assertTrue(activities.escalations.isEmpty(), "deadline must have moved to 27h")

        testEnv.sleep(Duration.ofHours(2)) // 28h
        eventually { assertEquals(1, activities.escalations.size) }
        assertAbout(t0 + 27 * HOUR_MS, activities.escalations[0].atMs, "breach at 24h + 3h paused")
        assertAbout(3 * HOUR_S, activities.escalations[0].pausedSecs, "paused total sent with breach")

        closeAndAwait("t3")
    }

    @Test
    fun `a second pause while already paused does not restart the pause`() {
        startTicket("t4", slaHours = 10)

        testEnv.sleep(Duration.ofHours(1))
        ticket("t4").pauseSla(PauseSlaSignal.newBuilder().setActorId("a").build())
        testEnv.sleep(Duration.ofHours(1))
        ticket("t4").pauseSla(PauseSlaSignal.newBuilder().setActorId("a").build()) // ignored
        testEnv.sleep(Duration.ofHours(1))
        ticket("t4").resumeSla(ResumeSlaSignal.newBuilder().setActorId("a").build())

        eventually { assertEquals(1, activities.pauseSyncs.size) }
        assertAbout(2 * HOUR_S, activities.pauseSyncs[0].totalPausedSeconds, "counted from the first pause")

        closeAndAwait("t4")
    }

    @Test
    fun `resolving during escalation stops the later tiers`() {
        startTicket("t5", slaHours = 1, tiers = listOf(2, 2))

        testEnv.sleep(Duration.ofHours(2)) // breach at 1h, tier 1 would be at 3h
        eventually { assertEquals(listOf(0), activities.escalations.map { it.tier }) }

        setStatus("t5", "RESOLVED")
        testEnv.sleep(Duration.ofHours(10))

        assertEquals(listOf(0), activities.escalations.map { it.tier })
        assertEquals(0, ticket("t5").getWorkflowDiagnostics().currentEscalationTier)

        closeAndAwait("t5")
    }

    @Test
    fun `resolving while paused records the paused time and never breaches`() {
        startTicket("t6", slaHours = 4)

        testEnv.sleep(Duration.ofHours(1))
        ticket("t6").pauseSla(PauseSlaSignal.newBuilder().setActorId("a").build())
        testEnv.sleep(Duration.ofHours(2))
        setStatus("t6", "RESOLVED")

        eventually { assertEquals(1, activities.pauseSyncs.size) }
        assertAbout(2 * HOUR_S, activities.pauseSyncs[0].totalPausedSeconds, "pause closed on resolution")

        testEnv.sleep(Duration.ofHours(24))
        assertTrue(activities.escalations.isEmpty())
        assertFalse(ticket("t6").getWorkflowDiagnostics().isSlaPaused)

        closeAndAwait("t6")
    }

    @Test
    fun `pauses after a breach are ignored`() {
        startTicket("t7", slaHours = 1, tiers = listOf(1))

        testEnv.sleep(Duration.ofMinutes(90)) // breached at 1h
        ticket("t7").pauseSla(PauseSlaSignal.newBuilder().setActorId("a").build())
        assertFalse(ticket("t7").getWorkflowDiagnostics().isSlaPaused)

        testEnv.sleep(Duration.ofHours(2))
        eventually { assertEquals(listOf(0, 1), activities.escalations.map { it.tier }) }

        closeAndAwait("t7")
    }

    @Test
    fun `declined ticket ends the workflow without escalating`() {
        startTicket("t8", slaHours = 24, tiers = listOf(4))

        setStatus("t8", "DECLINED")
        WorkflowStub.fromTyped(ticket("t8")).getResult(Void::class.java)

        assertTrue(activities.escalations.isEmpty())
    }

    // --- helpers ---

    /** Starts a ticket workflow the same way the Go backend does. Returns the start time (ms). */
    private fun startTicket(id: String, slaHours: Long, tiers: List<Long> = emptyList()): Long {
        val input = TicketWorkflowInput.newBuilder()
            .setTicketId(id)
            .setTicketNumber("RS-$id")
            .setTicketType("SERVICE")
            .setRegionCode("NRB")
            .setBranchCode("001")
            .setSrmUserId("srm-1")
            .setSlaHoursApplied(slaHours)
            .setReportedAtEpochMs(testEnv.currentTimeMillis())
            .addAllEscalationTierHours(tiers)
            .build()
        val stub = client.newWorkflowStub(
            TicketLifecycleWorkflow::class.java,
            WorkflowOptions.newBuilder()
                .setWorkflowId("ticket-$id")
                .setTaskQueue(TicketLifecycleWorkflowImpl.TASK_QUEUE)
                .build()
        )
        val startedAt = testEnv.currentTimeMillis()
        WorkflowStub.fromTyped(stub).start(input)
        return startedAt
    }

    private fun ticket(id: String): TicketLifecycleWorkflow =
        client.newWorkflowStub(TicketLifecycleWorkflow::class.java, "ticket-$id")

    private fun setStatus(id: String, status: String) =
        ticket(id).onStatusTransition(StatusTransitionSignal.newBuilder().setNewStatus(status).setActorId("officer-1").build())

    private fun closeAndAwait(id: String) {
        setStatus(id, "CLOSED")
        WorkflowStub.fromTyped(ticket(id)).getResult(Void::class.java)
    }

    /** Activities run on worker threads, so give them a moment to be recorded. */
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
        val tolerance = if (expected > 1_000_000_000L) 60_000L else 60L // ms for timestamps, s for durations
        assertTrue(abs(actual - expected) <= tolerance, "$what: expected ~$expected but was $actual")
    }

    companion object {
        private const val HOUR_S = 3_600L
        private const val HOUR_MS = 3_600_000L
    }
}

/**
 * Fake activities: record what the workflow asked for instead of calling the Go backend.
 * quarkus-temporal binds every TicketActivities implementor it finds to a worker, so in
 * @QuarkusTest runs this would clash with TicketActivitiesImpl on the default worker.
 * Parking it on its own worker keeps it out of the way.
 */
@TemporalActivity(workers = ["unit-test-fakes"])
class RecordingActivities : TicketActivities {
    data class Escalation(val tier: Int, val atMs: Long, val pausedSecs: Long, val srmUserId: String, val ticketNumber: String)
    data class PauseSync(val totalPausedSeconds: Long, val actorId: String)

    val escalations = CopyOnWriteArrayList<Escalation>()
    val pauseSyncs = CopyOnWriteArrayList<PauseSync>()

    override fun recordAndNotifySlaEscalation(
        ticketId: String, ticketNumber: String, escalationTier: Int,
        srmUserId: String, breachedAtEpochMs: Long, totalPausedSeconds: Long
    ): String {
        escalations += Escalation(escalationTier, breachedAtEpochMs, totalPausedSeconds, srmUserId, ticketNumber)
        return "IN_PROGRESS"
    }

    override fun syncPausedDurationToBackend(ticketId: String, totalPausedSeconds: Long, actorId: String) {
        pauseSyncs += PauseSync(totalPausedSeconds, actorId)
    }
}
