package com.tracom.lango.worker.activities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.tracom.lango.proto.v1.RecordSlaBreachRequest;
import com.tracom.lango.proto.v1.RecordSlaWarningRequest;
import com.tracom.lango.proto.v1.TicketInternalCallbackServiceGrpc;
import com.tracom.lango.worker.testsupport.FakeGoBackend;

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
class TicketActivitiesImplTest {

    private FakeGoBackend backend;
    private Server server;
    private ManagedChannel channel;
    private TicketActivitiesImpl activities;

    @BeforeEach
    void setUp() throws IOException {
        backend = new FakeGoBackend();
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor().addService(backend).build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();

        activities = new TicketActivitiesImpl();
        activities.goBackendClient = TicketInternalCallbackServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void escalationIsSentToBackendWithAllFields() {
        String status = activities.recordAndNotifySlaEscalation(
                "uuid-1", "RS10000000011772654400", 2, "srm-7", 1_772_654_400_000L, 3_600L);

        assertEquals("IN_PROGRESS", status);
        assertEquals(1, backend.breaches.size());
        RecordSlaBreachRequest r = backend.breaches.get(0);
        assertEquals("uuid-1", r.getTicketId());
        assertEquals("RS10000000011772654400", r.getTicketNumber());
        assertEquals(2, r.getEscalationTier());
        assertEquals("srm-7", r.getSrmUserId());
        assertEquals(1_772_654_400_000L, r.getBreachedAtEpochMs());
        assertEquals(3_600L, r.getTotalPausedSeconds());
    }

    @Test
    void pausedDurationIsSentToBackend() {
        activities.syncPausedDurationToBackend("uuid-1", 10_800L, "officer-1");

        assertEquals(1, backend.pauseSyncs.size());
        assertEquals("uuid-1", backend.pauseSyncs.get(0).getTicketId());
        assertEquals(10_800L, backend.pauseSyncs.get(0).getTotalPausedSeconds());
        assertEquals("officer-1", backend.pauseSyncs.get(0).getUpdatedByActorId());
    }

    @Test
    void warningIsSentToBackendWithAllFields() {
        activities.recordSlaWarning("uuid-1", "RS10000000011772654400", "srm-7", 75, 1_772_654_400_000L, 1_772_676_000_000L);

        assertEquals(1, backend.warnings.size());
        RecordSlaWarningRequest w = backend.warnings.get(0);
        assertEquals("uuid-1", w.getTicketId());
        assertEquals("RS10000000011772654400", w.getTicketNumber());
        assertEquals("srm-7", w.getSrmUserId());
        assertEquals(75, w.getWarningPercent());
        assertEquals(1_772_654_400_000L, w.getWarnedAtEpochMs());
        assertEquals(1_772_676_000_000L, w.getSlaDueAtEpochMs());
    }

    @Test
    void backendRejectionThrowsARetryableFailure() {
        backend.rejectRequests = true;

        ApplicationFailure failure = assertThrows(ApplicationFailure.class,
                () -> activities.recordAndNotifySlaEscalation("uuid-1", "RS1", 0, "srm-7", 0L, 0L));
        assertEquals("BACKEND_REJECTED", failure.getType());
        assertFalse(failure.isNonRetryable(), "Temporal must retry, not give up");

        assertThrows(ApplicationFailure.class,
                () -> activities.syncPausedDurationToBackend("uuid-1", 60L, "officer-1"));
        assertThrows(ApplicationFailure.class,
                () -> activities.recordSlaWarning("uuid-1", "RS1", "srm-7", 75, 0L, 0L));
    }

    @Test
    void backendDownThrowsSoTemporalRetries() {
        server.shutdownNow();

        assertThrows(StatusRuntimeException.class,
                () -> activities.recordAndNotifySlaEscalation("uuid-1", "RS1", 0, "srm-7", 0L, 0L));
    }
}
