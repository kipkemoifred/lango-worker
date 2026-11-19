package com.tracom.lango.worker.activities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.tracom.lango.proto.v1.ScheduleVisitRequest;
import com.tracom.lango.proto.v1.VisitCallbackServiceGrpc;
import com.tracom.lango.worker.testsupport.FakeVisitBackend;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.temporal.failure.ApplicationFailure;

/**
 * Activity tests against a fake Go backend over in-process gRPC: checks that
 * every field reaches the backend and that failures are surfaced so Temporal retries.
 */
class VisitActivitiesImplTest {

    private FakeVisitBackend backend;
    private Server server;
    private ManagedChannel channel;
    private VisitActivitiesImpl activities;

    @BeforeEach
    void setUp() throws IOException {
        backend = new FakeVisitBackend();
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor().addService(backend).build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();

        activities = new VisitActivitiesImpl();
        activities.goBackendClient = VisitCallbackServiceGrpc.newBlockingStub(channel);
        activities.windowDays = 7;
        activities.reminderDaysBeforeWindowEnd = 2;
        activities.zoneId = "Africa/Nairobi";
    }

    @AfterEach
    void tearDown() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void policyComesFromConfig() {
        assertEquals(new VisitPolicy(7, 2, "Africa/Nairobi"), activities.loadVisitPolicy());
    }

    @Test
    void scheduleIsSentToBackendWithAllFieldsAndReturnsTheVisitId() {
        String visitId = activities.scheduleVisit("cycle-1", "merchant-1", "outlet-1", "2026-12-01", "2026-12-07");

        assertEquals("visit-cycle-1-2026-12-01", visitId);
        ScheduleVisitRequest r = backend.scheduled.get(0);
        assertEquals("cycle-1", r.getCycleId());
        assertEquals("merchant-1", r.getMerchantId());
        assertEquals("outlet-1", r.getOutletId());
        assertEquals("2026-12-01", r.getDueDate());
        assertEquals("2026-12-07", r.getWindowEndDate());
    }

    @Test
    void reminderAndMissedAreSentToBackend() {
        activities.remindVisitDue("visit-1", "cycle-1", "2026-12-07");
        activities.markVisitMissed("visit-1", "cycle-1", "2026-12-01");

        assertEquals("visit-1", backend.reminders.get(0).getVisitId());
        assertEquals("2026-12-07", backend.reminders.get(0).getWindowEndDate());
        assertEquals("visit-1", backend.missed.get(0).getVisitId());
        assertEquals("cycle-1", backend.missed.get(0).getCycleId());
        assertEquals("2026-12-01", backend.missed.get(0).getDueDate());
    }

    @Test
    void backendRejectionThrowsARetryableFailure() {
        backend.rejectRequests = true;

        ApplicationFailure failure = assertThrows(ApplicationFailure.class,
                () -> activities.scheduleVisit("cycle-1", "merchant-1", "outlet-1", "2026-12-01", "2026-12-07"));
        assertEquals("BACKEND_REJECTED", failure.getType());
        assertFalse(failure.isNonRetryable(), "Temporal must retry, not give up");

        assertThrows(ApplicationFailure.class, () -> activities.remindVisitDue("visit-1", "cycle-1", "2026-12-07"));
        assertThrows(ApplicationFailure.class, () -> activities.markVisitMissed("visit-1", "cycle-1", "2026-12-01"));
    }

    @Test
    void backendDownThrowsSoTemporalRetries() {
        server.shutdownNow();

        assertThrows(StatusRuntimeException.class,
                () -> activities.scheduleVisit("cycle-1", "merchant-1", "outlet-1", "2026-12-01", "2026-12-07"));
    }
}
