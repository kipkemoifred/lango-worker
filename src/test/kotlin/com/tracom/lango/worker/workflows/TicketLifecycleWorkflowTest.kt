package com.tracom.lango.worker.workflows

import com.tracom.lango.proto.v1.PauseSlaSignal
import com.tracom.lango.proto.v1.ReopenedTicketState
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

    @Test
    fun `backend outage after a resume does not hold up the SLA clock`() {
        activities.backendDown = true
        val t0 = startTicket("t9", slaHours = 4)
        testEnv.sleep(Duration.ofHours(1))
        ticket("t9").pauseSla(PauseSlaSignal.newBuilder().setActorId("officer-1").build())
        testEnv.sleep(Duration.ofHours(1))
        ticket("t9").resumeSla(ResumeSlaSignal.newBuilder().setActorId("officer-1").build())

        // 3h of SLA left after the resume at t0+2h: the breach is due at t0+5h, report or no report
        testEnv.sleep(Duration.ofHours(4))
        eventually { assertEquals(1, activities.escalations.size) }
        assertAbout(t0 + 5 * HOUR_MS, activities.escalations[0].atMs, "breach on time despite the outage")
        assertTrue(activities.pauseSyncs.isEmpty(), "the paused-time report is still failing")

        // Closing waits for the paused total to reach the backend, then ends
        setStatus("t9", "CLOSED")
        activities.backendDown = false
        WorkflowStub.fromTyped(ticket("t9")).getResult(Void::class.java)
        assertAbout(HOUR_S, activities.pauseSyncs.last().totalPausedSeconds, "the 1h pause reached the backend")
    }

    @Test
    fun `diagnostics count down between events and show the deadline`() {
        val t0 = startTicket("t10", slaHours = 24)
        eventually { assertTrue(ticket("t10").getWorkflowDiagnostics().slaDueAtEpochMs > 0) }
        val first = ticket("t10").getWorkflowDiagnostics()
        assertAbout(t0 + 24 * HOUR_MS, first.slaDueAtEpochMs, "deadline while the clock runs")

        Thread.sleep(2_100) // real time passes with no event in between
        val later = ticket("t10").getWorkflowDiagnostics()
        assertTrue(
            later.remainingSlaSeconds <= first.remainingSlaSeconds - 2,
            "remaining time counts down: ${first.remainingSlaSeconds} -> ${later.remainingSlaSeconds}"
        )

        ticket("t10").pauseSla(PauseSlaSignal.newBuilder().setActorId("officer-1").build())
        eventually { assertEquals(0L, ticket("t10").getWorkflowDiagnostics().slaDueAtEpochMs, "no deadline while paused") }
        closeAndAwait("t10")
    }

    // --- BA DFD-2: warning before a breach (F20) ---

    @Test
    fun `warns the SRM once at the warning point`() {
        val t0 = startTicketWith("t11") { setSlaHoursApplied(24).setSlaWarningPercent(75) }

        testEnv.sleep(Duration.ofHours(17))
        assertTrue(activities.warnings.isEmpty(), "no warning before 75% of 24h (18h)")
        testEnv.sleep(Duration.ofHours(2))
        eventually { assertEquals(1, activities.warnings.size) }
        val w = activities.warnings[0]
        assertAbout(t0 + 18 * HOUR_MS, w.atMs, "warning at 75% used")
        assertAbout(t0 + 24 * HOUR_MS, w.dueAtMs, "warning carries the deadline")
        assertEquals(75, w.percent)
        assertEquals("srm-1", w.srmUserId)
        assertTrue(ticket("t11").getWorkflowDiagnostics().slaWarningSent)

        testEnv.sleep(Duration.ofHours(10))
        eventually { assertEquals(1, activities.escalations.size) }
        assertEquals(1, activities.warnings.size, "warned once only")
        closeAndAwait("t11")
    }

    @Test
    fun `a pause pushes the warning back and a ticket resolved first is never warned`() {
        val t0 = startTicketWith("t12") { setSlaHoursApplied(24).setSlaWarningPercent(75) }
        testEnv.sleep(Duration.ofHours(10))
        ticket("t12").pauseSla(PauseSlaSignal.newBuilder().setActorId("officer-1").build())
        testEnv.sleep(Duration.ofHours(2))
        ticket("t12").resumeSla(ResumeSlaSignal.newBuilder().setActorId("officer-1").build())
        testEnv.sleep(Duration.ofHours(7)) // 17h of SLA used
        assertTrue(activities.warnings.isEmpty())
        testEnv.sleep(Duration.ofHours(2))
        eventually { assertEquals(1, activities.warnings.size) }
        assertAbout(t0 + 20 * HOUR_MS, activities.warnings[0].atMs, "18h used plus the 2h pause")
        closeAndAwait("t12")

        startTicketWith("t13") { setSlaHoursApplied(24).setSlaWarningPercent(75) }
        testEnv.sleep(Duration.ofHours(10))
        setStatus("t13", "RESOLVED")
        testEnv.sleep(Duration.ofHours(48))
        assertEquals(1, activities.warnings.size, "no warning for t13, resolved before 18h")
        closeAndAwait("t13")
    }

    @Test
    fun `a warning the backend can't receive doesn't delay the breach`() {
        activities.warningBackendDown = true
        val t0 = startTicketWith("t14") { setSlaHoursApplied(4).setSlaWarningPercent(50) }

        testEnv.sleep(Duration.ofHours(5))
        eventually { assertEquals(1, activities.escalations.size) }
        assertAbout(t0 + 4 * HOUR_MS, activities.escalations[0].atMs, "breach on time while the warning retries")
        assertTrue(activities.warnings.isEmpty())

        // Closing waits for the warning to be delivered, then ends
        setStatus("t14", "CLOSED")
        activities.warningBackendDown = false
        WorkflowStub.fromTyped(ticket("t14")).getResult(Void::class.java)
        assertEquals(1, activities.warnings.size)
    }

    // --- BA DFD-2: SLA calendar (F04), deadline computed by the backend ---

    @Test
    fun `runs to a business-time deadline sent by the backend`() {
        val now = testEnv.currentTimeMillis()
        startTicketWith("t15") { setSlaHoursApplied(0).setSlaDueAtEpochMs(now + 10 * HOUR_MS) }
        eventually { assertAbout(now + 10 * HOUR_MS, ticket("t15").getWorkflowDiagnostics().slaDueAtEpochMs, "deadline as sent") }

        testEnv.sleep(Duration.ofHours(9))
        assertTrue(activities.escalations.isEmpty())
        testEnv.sleep(Duration.ofHours(2))
        eventually { assertEquals(1, activities.escalations.size) }
        assertAbout(now + 10 * HOUR_MS, activities.escalations[0].atMs, "breach at the backend's deadline")
        closeAndAwait("t15")
    }

    @Test
    fun `a resume can carry a new business-time deadline`() {
        val now = testEnv.currentTimeMillis()
        startTicketWith("t16") { setSlaHoursApplied(0).setSlaDueAtEpochMs(now + 10 * HOUR_MS) }
        testEnv.sleep(Duration.ofHours(2))
        ticket("t16").pauseSla(PauseSlaSignal.newBuilder().setActorId("officer-1").build())
        testEnv.sleep(Duration.ofHours(1))
        // e.g. the pause ran into the weekend: the backend moves the deadline by 10h, not 1h
        ticket("t16").resumeSla(
            ResumeSlaSignal.newBuilder().setActorId("officer-1").setSlaDueAtEpochMs(now + 20 * HOUR_MS).build()
        )

        testEnv.sleep(Duration.ofHours(10)) // now+13h: past the old deadline, before the new one
        assertTrue(activities.escalations.isEmpty())
        testEnv.sleep(Duration.ofHours(8))
        eventually { assertEquals(1, activities.escalations.size) }
        assertAbout(now + 20 * HOUR_MS, activities.escalations[0].atMs, "breach at the new deadline")
        closeAndAwait("t16")
    }

    // --- BA DFD-2: reopened tickets (2.5) ---

    @Test
    fun `reopening a resolved ticket continues its remaining SLA`() {
        startTicketWith("t17") { setSlaHoursApplied(24) }
        testEnv.sleep(Duration.ofHours(10))
        setStatus("t17", "RESOLVED")
        testEnv.sleep(Duration.ofHours(48))

        setStatus("t17", "REOPENED")
        val reopenedAt = testEnv.currentTimeMillis()
        eventually {
            val d = ticket("t17").getWorkflowDiagnostics()
            assertEquals("REOPENED", d.currentStatus)
            assertEquals(1, d.reopenCount)
        }
        testEnv.sleep(Duration.ofHours(13))
        assertTrue(activities.escalations.isEmpty(), "14h were left when it was resolved")
        testEnv.sleep(Duration.ofHours(2))
        eventually { assertEquals(1, activities.escalations.size) }
        assertAbout(reopenedAt + 14 * HOUR_MS, activities.escalations[0].atMs, "breach after the remaining 14h")
        closeAndAwait("t17")
    }

    @Test
    fun `reopening a breached ticket carries on up the escalation ladder`() {
        startTicketWith("t18") { setSlaHoursApplied(1).addAllEscalationTierHours(listOf(4L, 8L)) }
        testEnv.sleep(Duration.ofHours(2))
        eventually { assertEquals(listOf(0), activities.escalations.map { it.tier }) }
        setStatus("t18", "RESOLVED")
        testEnv.sleep(Duration.ofHours(24))
        assertEquals(1, activities.escalations.size, "nothing more while resolved")

        setStatus("t18", "REOPENED")
        val reopenedAt = testEnv.currentTimeMillis()
        testEnv.sleep(Duration.ofHours(13))
        eventually { assertEquals(listOf(0, 1, 2), activities.escalations.map { it.tier }) }
        assertAbout(reopenedAt + 4 * HOUR_MS, activities.escalations[1].atMs, "tier 1 four hours after the reopen")
        assertEquals("PAST_SLA", ticket("t18").getWorkflowDiagnostics().slaStatus)
        closeAndAwait("t18")
    }

    @Test
    fun `REOPENED is ignored unless the ticket is resolved`() {
        startTicketWith("t19") { setSlaHoursApplied(24) }
        setStatus("t19", "REOPENED")
        testEnv.sleep(Duration.ofMinutes(1))
        eventually {
            val d = ticket("t19").getWorkflowDiagnostics()
            assertEquals("RAISED", d.currentStatus)
            assertEquals(0, d.reopenCount)
        }
        closeAndAwait("t19")
    }

    @Test
    fun `a ticket reopened after closure carries on from its closed run`() {
        val t0 = startTicketWith("t20") {
            setSlaHoursApplied(24).setSlaWarningPercent(75).setReopened(
                ReopenedTicketState.newBuilder()
                    .setRemainingSlaSeconds(5 * HOUR_S).setTotalPausedSeconds(HOUR_S)
                    .setReopenCount(1).setSlaWarningSent(true)
            )
        }
        eventually {
            val d = ticket("t20").getWorkflowDiagnostics()
            assertEquals("REOPENED", d.currentStatus)
            assertEquals(2, d.reopenCount)
            assertEquals(HOUR_S, d.accumulatedPausedSeconds)
        }
        testEnv.sleep(Duration.ofHours(4))
        assertTrue(activities.escalations.isEmpty())
        testEnv.sleep(Duration.ofHours(2))
        eventually { assertEquals(1, activities.escalations.size) }
        assertAbout(t0 + 5 * HOUR_MS, activities.escalations[0].atMs, "breach after the 5h carried over")
        assertTrue(activities.warnings.isEmpty(), "already warned before it was closed")
        closeAndAwait("t20")

        // Closed while breached at tier 0: the reopened run climbs to tier 1, without re-alerting the SRM
        val t1 = startTicketWith("t21") {
            setSlaHoursApplied(1).addAllEscalationTierHours(listOf(4L)).setReopened(
                ReopenedTicketState.newBuilder().setSlaBreached(true).setEscalationTier(0).setReopenCount(0)
            )
        }
        testEnv.sleep(Duration.ofHours(5))
        eventually { assertEquals(listOf(0, 1), activities.escalations.map { it.tier }) }
        assertAbout(t1 + 4 * HOUR_MS, activities.escalations[1].atMs, "tier 1 four hours after the reopen")
        closeAndAwait("t21")
    }

    // --- helpers ---

    /** Starts a ticket with the newer optional fields set by [configure]. Returns the start time (ms). */
    private fun startTicketWith(id: String, configure: TicketWorkflowInput.Builder.() -> Unit): Long {
        val input = TicketWorkflowInput.newBuilder()
            .setTicketId(id)
            .setTicketNumber("RS-$id")
            .setTicketType("SERVICE")
            .setRegionCode("NRB")
            .setBranchCode("001")
            .setSrmUserId("srm-1")
            .setReportedAtEpochMs(testEnv.currentTimeMillis())
            .apply(configure)
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
    data class Warning(val percent: Int, val atMs: Long, val dueAtMs: Long, val srmUserId: String)

    val escalations = CopyOnWriteArrayList<Escalation>()
    val pauseSyncs = CopyOnWriteArrayList<PauseSync>()
    val warnings = CopyOnWriteArrayList<Warning>()

    /** When true, the paused-time report fails like a backend outage, and Temporal keeps retrying it. */
    @Volatile
    var backendDown = false

    /** When true, the SLA warning call fails like a backend outage, and Temporal keeps retrying it. */
    @Volatile
    var warningBackendDown = false

    override fun recordAndNotifySlaEscalation(
        ticketId: String, ticketNumber: String, escalationTier: Int,
        srmUserId: String, breachedAtEpochMs: Long, totalPausedSeconds: Long
    ): String {
        escalations += Escalation(escalationTier, breachedAtEpochMs, totalPausedSeconds, srmUserId, ticketNumber)
        return "IN_PROGRESS"
    }

    override fun recordSlaWarning(
        ticketId: String, ticketNumber: String, srmUserId: String,
        warningPercent: Int, warnedAtEpochMs: Long, slaDueAtEpochMs: Long
    ) {
        if (warningBackendDown) throw IllegalStateException("backend unavailable")
        warnings += Warning(warningPercent, warnedAtEpochMs, slaDueAtEpochMs, srmUserId)
    }

    override fun syncPausedDurationToBackend(ticketId: String, totalPausedSeconds: Long, actorId: String) {
        if (backendDown) throw IllegalStateException("backend unavailable")
        pauseSyncs += PauseSync(totalPausedSeconds, actorId)
    }
}
