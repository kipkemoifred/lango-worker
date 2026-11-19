package com.tracom.lango.worker.workflows

import com.tracom.lango.proto.v1.VisitCycleDeactivatedSignal
import com.tracom.lango.proto.v1.VisitCycleDiagnostics
import com.tracom.lango.proto.v1.VisitCycleWorkflowInput
import com.tracom.lango.proto.v1.VisitFrequencyChangedSignal
import com.tracom.lango.proto.v1.VisitOutcomeSignal
import com.tracom.lango.worker.activities.VisitActivities
import com.tracom.lango.worker.activities.VisitPolicy
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
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.math.abs

/**
 * Visit-cycle logic on Temporal's in-memory server with time skipping, so months of
 * visits take seconds. Activities are replaced by [RecordingVisitActivities].
 *
 * Test policy: 7-day window, reminder 2 days before it closes, Africa/Nairobi dates.
 */
@Timeout(60)
class VisitCycleWorkflowTest {

    private lateinit var testEnv: TestWorkflowEnvironment
    private lateinit var client: WorkflowClient
    private lateinit var activities: RecordingVisitActivities
    private lateinit var today: LocalDate

    @BeforeEach
    fun setUp() {
        testEnv = TestWorkflowEnvironment.newInstance()
        LangoSearchAttributes.register(testEnv)

        val worker = testEnv.newWorker(VisitCycleWorkflowImpl.TASK_QUEUE)
        worker.registerWorkflowImplementationTypes(VisitCycleWorkflowImpl::class.java)
        activities = RecordingVisitActivities()
        worker.registerActivitiesImplementations(activities)

        testEnv.start()
        client = testEnv.workflowClient
        today = currentDate()
    }

    @AfterEach
    fun tearDown() {
        testEnv.close()
    }

    @Test
    fun `each visit is created on its due date and the cycle carries on`() {
        val d0 = today.plusDays(1)
        start("c1", anchor = d0, intervalDays = 30)

        assertTrue(activities.scheduled.isEmpty(), "nothing before the first due date")
        for (n in 0..2) {
            val due = d0.plusDays(30L * n)
            testEnv.sleep(Duration.ofDays(if (n == 0) 1 else 30))
            eventually { assertEquals(due.toString(), activities.scheduled.last().dueDate) }

            val visit = activities.scheduled.last()
            assertEquals(due.plusDays(6).toString(), visit.windowEndDate, "7-day window, inclusive end")
            assertAbout(startOfDayMs(due), visit.atMs, "visit ${n + 1} created at the start of its due date")
            val run = runId("c1")
            outcome("c1", visit.visitId, "COMPLETED")
            awaitNextRun("c1", run)
            assertEquals(due.toString(), diagnostics("c1").lastVisitDueDate)
        }

        assertEquals(3, activities.scheduled.size)
        assertTrue(activities.reminders.isEmpty() && activities.missed.isEmpty(), "every visit was done in time")
        val d = diagnostics("c1")
        assertEquals("COMPLETED", d.lastVisitOutcome)
        assertEquals("WAITING_FOR_DUE_DATE", d.phase)
        assertEquals(d0.plusDays(90).toString(), d.nextDueDate)
    }

    @Test
    fun `visit with no outcome gets a reminder and is then marked missed`() {
        val d0 = today.plusDays(1)
        start("c2", anchor = d0, intervalDays = 30)

        testEnv.sleep(Duration.ofDays(1))
        eventually { assertEquals(1, activities.scheduled.size) }
        assertEquals("VISIT_OPEN", diagnostics("c2").phase)

        testEnv.sleep(Duration.ofDays(4))
        assertTrue(activities.reminders.isEmpty(), "reminder only 2 days before the window closes")

        testEnv.sleep(Duration.ofDays(1))
        eventually { assertEquals(1, activities.reminders.size) }
        assertAbout(startOfDayMs(d0.plusDays(5)), activities.reminders[0].atMs, "reminder 2 days before the window closes")
        assertTrue(activities.missed.isEmpty())

        val run = runId("c2")
        testEnv.sleep(Duration.ofDays(2))
        eventually { assertEquals(1, activities.missed.size) }
        awaitNextRun("c2", run)
        assertAbout(startOfDayMs(d0.plusDays(7)), activities.missed[0].atMs, "missed when the window closes")
        assertEquals(d0.toString(), activities.missed[0].dueDate)

        eventually {
            val d = diagnostics("c2")
            assertEquals("MISSED", d.lastVisitOutcome)
            assertEquals(d0.plusDays(30).toString(), d.nextDueDate, "a missed visit doesn't shift the schedule")
        }
    }

    @Test
    fun `waived visit is neither reminded nor missed`() {
        val d0 = today.plusDays(1)
        start("c3", anchor = d0, intervalDays = 30)
        testEnv.sleep(Duration.ofDays(1))
        eventually { assertEquals(1, activities.scheduled.size) }

        outcome("c3", activities.scheduled[0].visitId, "WAIVED")
        testEnv.sleep(Duration.ofDays(10))

        assertTrue(activities.reminders.isEmpty() && activities.missed.isEmpty())
        assertEquals("WAIVED", diagnostics("c3").lastVisitOutcome)
    }

    @Test
    fun `outcome for a different visit is ignored`() {
        start("c4", anchor = today.plusDays(1), intervalDays = 30)
        testEnv.sleep(Duration.ofDays(1))
        eventually { assertEquals(1, activities.scheduled.size) }

        outcome("c4", "visit-from-another-cycle", "COMPLETED")
        outcome("c4", activities.scheduled[0].visitId, "ASSIGNED") // not a closing outcome
        testEnv.sleep(Duration.ofDays(8))

        eventually { assertEquals(1, activities.missed.size) }
    }

    @Test
    fun `visits already past their window are skipped, an open one is created at once`() {
        // weekly visits from 20 days ago: the first two windows are over, the third (6 days ago) is still open
        start("c5", anchor = today.minusDays(20), intervalDays = 7)

        eventually { assertEquals(1, activities.scheduled.size) }
        assertEquals(today.minusDays(6).toString(), activities.scheduled[0].dueDate)
        assertEquals(today.toString(), activities.scheduled[0].windowEndDate)
        assertTrue(activities.missed.isEmpty(), "skipped visits are not created and marked missed")
    }

    @Test
    fun `frequency change while waiting reschedules from the new anchor and interval`() {
        start("c6", anchor = today.plusDays(10), intervalDays = 90)
        val run = runId("c6")
        frequency("c6", intervalDays = 14, anchor = today.plusDays(3))
        awaitNextRun("c6", run)

        eventually {
            val d = diagnostics("c6")
            assertEquals(14, d.intervalDays)
            assertEquals(today.plusDays(3).toString(), d.nextDueDate)
        }
        testEnv.sleep(Duration.ofDays(3))
        eventually { assertEquals(listOf(today.plusDays(3).toString()), activities.scheduled.map { it.dueDate }) }
    }

    @Test
    fun `frequency change during an open visit waits for it and never reopens it`() {
        val d0 = today.plusDays(1)
        start("c7", anchor = d0, intervalDays = 30)
        testEnv.sleep(Duration.ofDays(1))
        eventually { assertEquals(1, activities.scheduled.size) }

        // daily from the same anchor: the new schedule's first date is the visit that is open now
        frequency("c7", intervalDays = 1, anchor = d0)
        eventually { assertTrue(diagnostics("c7").frequencyChangePending) }
        val run = runId("c7")
        outcome("c7", activities.scheduled[0].visitId, "COMPLETED")
        awaitNextRun("c7", run)
        assertEquals(d0.plusDays(1).toString(), diagnostics("c7").nextDueDate)

        testEnv.sleep(Duration.ofDays(1))
        eventually { assertEquals(listOf(d0.toString(), d0.plusDays(1).toString()), activities.scheduled.map { it.dueDate }.distinct()) }
        assertEquals(1, activities.scheduled.count { it.dueDate == d0.toString() }, "the completed visit is not created again")
    }

    @Test
    fun `deactivation ends the cycle without creating or missing visits`() {
        start("c8", anchor = today.plusDays(5), intervalDays = 30)
        deactivate("c8")
        awaitEnd("c8")
        testEnv.sleep(Duration.ofDays(40))
        assertTrue(activities.scheduled.isEmpty())

        start("c9", anchor = currentDate().plusDays(1), intervalDays = 30)
        testEnv.sleep(Duration.ofDays(1))
        eventually { assertEquals(1, activities.scheduled.size) }
        deactivate("c9") // while the visit is open: the backend cancels it, the worker just stops
        awaitEnd("c9")
        testEnv.sleep(Duration.ofDays(10))
        assertTrue(activities.missed.isEmpty())
    }

    // --- helpers ---

    /** Starts a cycle the way the backend would once the merchant is activated. */
    private fun start(id: String, anchor: LocalDate, intervalDays: Int) {
        val stub = client.newWorkflowStub(
            VisitCycleWorkflow::class.java,
            WorkflowOptions.newBuilder()
                .setWorkflowId("visit-cycle-$id")
                .setTaskQueue(VisitCycleWorkflowImpl.TASK_QUEUE)
                .build()
        )
        WorkflowStub.fromTyped(stub).start(
            VisitCycleWorkflowInput.newBuilder()
                .setCycleId(id)
                .setMerchantId("merchant-$id")
                .setOutletId("outlet-$id")
                .setRegionCode("NRB")
                .setBranchCode("001")
                .setIntervalDays(intervalDays)
                .setAnchorDate(anchor.toString())
                .build()
        )
    }

    private fun cycle(id: String): VisitCycleWorkflow =
        client.newWorkflowStub(VisitCycleWorkflow::class.java, "visit-cycle-$id")

    private fun outcome(id: String, visitId: String, outcome: String) =
        cycle(id).onVisitOutcome(VisitOutcomeSignal.newBuilder().setVisitId(visitId).setOutcome(outcome).setActorId("officer-1").build())

    private fun frequency(id: String, intervalDays: Int, anchor: LocalDate) =
        cycle(id).onFrequencyChanged(
            VisitFrequencyChangedSignal.newBuilder()
                .setIntervalDays(intervalDays).setAnchorDate(anchor.toString()).setActorId("admin-1").build()
        )

    private fun deactivate(id: String) =
        cycle(id).onCycleDeactivated(VisitCycleDeactivatedSignal.newBuilder().setActorId("admin-1").setReason("merchant closed").build())

    private fun diagnostics(id: String): VisitCycleDiagnostics = cycle(id).getVisitCycleDiagnostics()

    private fun awaitEnd(id: String) {
        WorkflowStub.fromTyped(cycle(id)).getResult(Void::class.java)
    }

    private fun runId(id: String): String = WorkflowStub.fromTyped(cycle(id)).describe().execution.runId

    /**
     * Waits until the cycle has continued-as-new past [previousRunId]. Querying exactly while a
     * run hands over to the next one trips up the in-memory test server's clock.
     */
    private fun awaitNextRun(id: String, previousRunId: String) =
        eventually { assertTrue(runId(id) != previousRunId, "cycle $id should have continued-as-new") }

    private fun currentDate(): LocalDate = LocalDate.ofInstant(Instant.ofEpochMilli(testEnv.currentTimeMillis()), NAIROBI)

    private fun startOfDayMs(date: LocalDate): Long = date.atStartOfDay(NAIROBI).toInstant().toEpochMilli()

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
        private val NAIROBI: ZoneId = ZoneId.of("Africa/Nairobi")
    }
}

data class ScheduledVisit(val visitId: String, val dueDate: String, val windowEndDate: String, val atMs: Long)
data class VisitReminder(val visitId: String, val windowEndDate: String, val atMs: Long)
data class MissedVisit(val visitId: String, val dueDate: String, val atMs: Long)

/**
 * Fake visit activities: record what the workflow asked for instead of calling the Go backend.
 * Parked on its own worker for the same reason as [RecordingActivities].
 */
@TemporalActivity(workers = ["unit-test-fakes"])
class RecordingVisitActivities(
    private val policy: VisitPolicy = VisitPolicy(7, 2, "Africa/Nairobi")
) : VisitActivities {

    val scheduled = CopyOnWriteArrayList<ScheduledVisit>()
    val reminders = CopyOnWriteArrayList<VisitReminder>()
    val missed = CopyOnWriteArrayList<MissedVisit>()

    override fun loadVisitPolicy(): VisitPolicy = policy

    override fun scheduleVisit(cycleId: String, merchantId: String, outletId: String, dueDate: String, windowEndDate: String): String {
        // like the real backend: the same (cycle, due date) always gives the same visit
        val visitId = "visit-$cycleId-$dueDate"
        scheduled += ScheduledVisit(visitId, dueDate, windowEndDate, io.temporal.activity.Activity.getExecutionContext().info.scheduledTimestamp)
        return visitId
    }

    override fun remindVisitDue(visitId: String, cycleId: String, windowEndDate: String) {
        reminders += VisitReminder(visitId, windowEndDate, io.temporal.activity.Activity.getExecutionContext().info.scheduledTimestamp)
    }

    override fun markVisitMissed(visitId: String, cycleId: String, dueDate: String) {
        missed += MissedVisit(visitId, dueDate, io.temporal.activity.Activity.getExecutionContext().info.scheduledTimestamp)
    }
}
