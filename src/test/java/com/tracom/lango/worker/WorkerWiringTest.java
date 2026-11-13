package com.tracom.lango.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.time.Duration;

import jakarta.inject.Inject;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.tracom.lango.proto.v1.RecordSlaBreachRequest;
import com.tracom.lango.proto.v1.StatusTransitionSignal;
import com.tracom.lango.proto.v1.TicketWorkflowInput;
import com.tracom.lango.worker.testsupport.FakeGoBackend;
import com.tracom.lango.worker.testsupport.LangoSearchAttributes;
import com.tracom.lango.worker.workflows.TicketLifecycleWorkflow;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.quarkus.test.junit.QuarkusTest;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.testing.TestWorkflowEnvironment;

/**
 * End-to-end inside the real Quarkus app: the worker picks the workflow up from
 * lango-tickets-tq, the real activity bean runs, and its @GrpcClient reaches a
 * fake Go backend on localhost:19090 (see %test config). Temporal itself is the
 * in-memory test server, so no Docker is needed.
 */
@QuarkusTest
class WorkerWiringTest {

    static final int FAKE_BACKEND_PORT = 19090;
    static final FakeGoBackend BACKEND = new FakeGoBackend();
    static Server backendServer;

    @Inject
    WorkflowClient client;

    @Inject
    TestWorkflowEnvironment testEnv;

    @BeforeAll
    static void startFakeBackend() throws IOException {
        backendServer = ServerBuilder.forPort(FAKE_BACKEND_PORT).addService(BACKEND).build().start();
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

    private static void waitForBreaches(int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (BACKEND.breaches.size() < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(expected, BACKEND.breaches.size(), "breach calls received by fake backend");
    }
}
