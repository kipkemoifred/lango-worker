package com.tracom.lango.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Map;

import jakarta.inject.Inject;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.tracom.lango.proto.v1.OnboardingStageDecisionSignal;
import com.tracom.lango.proto.v1.ProvisionMerchantRequest;
import com.tracom.lango.proto.v1.RecordSlaBreachRequest;
import com.tracom.lango.proto.v1.ScheduleVisitRequest;
import com.tracom.lango.proto.v1.StatusTransitionSignal;
import com.tracom.lango.proto.v1.TicketWorkflowInput;
import com.tracom.lango.proto.v1.VisitCycleDeactivatedSignal;
import com.tracom.lango.proto.v1.VisitCycleWorkflowInput;
import com.tracom.lango.worker.testsupport.FakeGoBackend;
import com.tracom.lango.worker.testsupport.FakeOnboardingBackend;
import com.tracom.lango.worker.testsupport.FakeVisitBackend;
import com.tracom.lango.worker.testsupport.LangoSearchAttributes;
import com.tracom.lango.worker.workflows.MerchantOnboardingWorkflow;
import com.tracom.lango.worker.workflows.TicketLifecycleWorkflow;
import com.tracom.lango.worker.workflows.VisitCycleWorkflow;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.quarkus.test.junit.QuarkusTest;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.testing.TestWorkflowEnvironment;

/**
 * End-to-end inside the real Quarkus app: each worker picks its workflow up from
 * its own queue (lango-tickets-tq, lango-onboarding-tq, lango-visits-tq), the real activity beans
 * run, and their @GrpcClient reaches a fake Go backend on localhost:19090 (see
 * %test config). Temporal itself is the in-memory test server, so no Docker is needed.
 */
@QuarkusTest
class WorkerWiringTest {

    static final int FAKE_BACKEND_PORT = 19090;
    static final FakeGoBackend BACKEND = new FakeGoBackend();
    static final FakeOnboardingBackend ONBOARDING_BACKEND = new FakeOnboardingBackend();
    static final FakeVisitBackend VISIT_BACKEND = new FakeVisitBackend();
    static Server backendServer;

    @Inject
    WorkflowClient client;

    @Inject
    TestWorkflowEnvironment testEnv;

    @BeforeAll
    static void startFakeBackend() throws IOException {
        backendServer = ServerBuilder.forPort(FAKE_BACKEND_PORT).addService(BACKEND).addService(ONBOARDING_BACKEND).addService(VISIT_BACKEND).build().start();
    }

    @AfterAll
    static void stopFakeBackend() {
        backendServer.shutdownNow();
    }

    @Test
    void breachTravelsFromWorkflowThroughActivityToTheBackend() throws InterruptedException {
        LangoSearchAttributes.register(testEnv);

        TicketLifecycleWorkflow workflow = client.newWorkflowStub(TicketLifecycleWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId("ticket-wiring-1")
                        .setTaskQueue("lango-tickets-tq")
                        .build());
        WorkflowStub.fromTyped(workflow).start(TicketWorkflowInput.newBuilder()
                .setTicketId("wiring-1")
                .setTicketNumber("RS10000000011772654400")
                .setTicketType("SERVICE")
                .setSrmUserId("srm-1")
                .setSlaHoursApplied(1)
                .build());

        testEnv.sleep(Duration.ofHours(2));
        waitForBreaches(1);

        RecordSlaBreachRequest breach = BACKEND.breaches.get(0);
        assertEquals("wiring-1", breach.getTicketId());
        assertEquals(0, breach.getEscalationTier());
        assertEquals("srm-1", breach.getSrmUserId());

        workflow.onStatusTransition(StatusTransitionSignal.newBuilder().setNewStatus("CLOSED").build());
        WorkflowStub.fromTyped(workflow).getResult(Void.class);
    }

    @Test
    void onboardingRunsOnItsOwnQueueAndProvisionsThroughTheBackend() throws java.util.concurrent.TimeoutException {
        LangoSearchAttributes.register(testEnv);

        // Started exactly like the Go RecruitMerchant handler: untyped, with a JSON map
        client.newUntypedWorkflowStub("MerchantOnboardingWorkflow",
                WorkflowOptions.newBuilder()
                        .setWorkflowId("onboarding-wiring-1")
                        .setTaskQueue("lango-onboarding-tq")
                        .build())
                .start(Map.of(
                        "applicationId", "app-wiring-1",
                        "ticketId", "ticket-wiring-1",
                        "ticketNumber", "RS10000000021772654400",
                        "recruitingAgentId", "agent-1",
                        "branchCode", "001",
                        "regionCode", "NRB"));

        MerchantOnboardingWorkflow application = client.newWorkflowStub(MerchantOnboardingWorkflow.class,
                "onboarding-wiring-1");
        for (String stage : new String[] { "AWAITING_REVIEW", "COMPLIANCE_REVIEW", "ICT_PROVISIONING" }) {
            application.onStageDecision(OnboardingStageDecisionSignal.newBuilder()
                    .setStage(stage).setOutcome("APPROVE").setActorId("reviewer-1").build());
        }
        WorkflowStub.fromTyped(application).getResult(30, java.util.concurrent.TimeUnit.SECONDS, Void.class);

        assertEquals(1, ONBOARDING_BACKEND.provisions.size(), "provision calls received by fake backend");
        ProvisionMerchantRequest provision = ONBOARDING_BACKEND.provisions.get(0);
        assertEquals("app-wiring-1", provision.getApplicationId());
        assertEquals("ticket-wiring-1", provision.getTicketId());
        assertEquals("reviewer-1", provision.getApprovedByActorId());
        assertEquals("ACTIVATED", application.getOnboardingDiagnostics().getCurrentStage());
    }

    @Test
    void visitCycleRunsOnItsOwnQueueAndSchedulesThroughTheBackend() throws Exception {
        LangoSearchAttributes.register(testEnv);
        LocalDate today = LocalDate.now(ZoneId.of("Africa/Nairobi"));

        VisitCycleWorkflow cycle = client.newWorkflowStub(VisitCycleWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId("visit-cycle-wiring-1")
                        .setTaskQueue("lango-visits-tq")
                        .build());
        WorkflowStub.fromTyped(cycle).start(VisitCycleWorkflowInput.newBuilder()
                .setCycleId("cycle-wiring-1")
                .setMerchantId("merchant-1")
                .setOutletId("outlet-1")
                .setIntervalDays(30)
                .setAnchorDate(today.toString()) // first visit is due today
                .build());

        long deadline = System.currentTimeMillis() + 10_000;
        while (VISIT_BACKEND.scheduled.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(1, VISIT_BACKEND.scheduled.size(), "schedule calls received by fake backend");
        ScheduleVisitRequest visit = VISIT_BACKEND.scheduled.get(0);
        assertEquals("cycle-wiring-1", visit.getCycleId());
        assertEquals(today.toString(), visit.getDueDate());
        assertEquals(today.plusDays(6).toString(), visit.getWindowEndDate(), "default 7-day window from config");

        cycle.onCycleDeactivated(VisitCycleDeactivatedSignal.newBuilder().setActorId("admin-1").build());
        WorkflowStub.fromTyped(cycle).getResult(30, java.util.concurrent.TimeUnit.SECONDS, Void.class);
    }

    private static void waitForBreaches(int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (BACKEND.breaches.size() < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(expected, BACKEND.breaches.size(), "breach calls received by fake backend");
    }
}
