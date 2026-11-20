package com.tracom.lango.worker.activities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.tracom.lango.proto.v1.OnboardingCallbackServiceGrpc;
import com.tracom.lango.proto.v1.OnboardingStageOverdueRequest;
import com.tracom.lango.worker.testsupport.FakeOnboardingBackend;

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
class OnboardingActivitiesImplTest {

    private FakeOnboardingBackend backend;
    private Server server;
    private ManagedChannel channel;
    private OnboardingActivitiesImpl activities;

    @BeforeEach
    void setUp() throws IOException {
        backend = new FakeOnboardingBackend();
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor().addService(backend).build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();

        activities = new OnboardingActivitiesImpl();
        activities.goBackendClient = OnboardingCallbackServiceGrpc.newBlockingStub(channel);
        activities.reviewDeadlineHours = 24;
        activities.complianceDeadlineHours = 48;
        activities.inspectionDeadlineHours = 72;
        activities.inspectionApprovalDeadlineHours = 24;
        activities.ictDeadlineHours = 12;
        activities.amendmentDeadlineHours = 168;
        activities.escalationTierHours = List.of(4L, 8L);
        activities.maxAmendmentCycles = 3;
    }

    @AfterEach
    void tearDown() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void policyComesFromConfig() {
        assertEquals(new OnboardingPolicy(24, 48, 72, 24, 12, 168, List.of(4L, 8L), 3), activities.loadOnboardingPolicy());
    }

    @Test
    void overdueAlertIsSentToBackendWithAllFields() {
        activities.notifyStageOverdue("app-1", "RS10000000011772654400", "COMPLIANCE_REVIEW", 1,
                "001", "NRB", 1_772_654_400_000L);

        assertEquals(1, backend.overdueAlerts.size());
        OnboardingStageOverdueRequest r = backend.overdueAlerts.get(0);
        assertEquals("app-1", r.getApplicationId());
        assertEquals("RS10000000011772654400", r.getTicketNumber());
        assertEquals("COMPLIANCE_REVIEW", r.getStage());
        assertEquals(1, r.getEscalationTier());
        assertEquals("001", r.getBranchCode());
        assertEquals("NRB", r.getRegionCode());
        assertEquals(1_772_654_400_000L, r.getOverdueAtEpochMs());
    }

    @Test
    void declineIsSentToBackend() {
        activities.declineApplication("app-1", "RETURNED_FOR_AMENDMENT", "AMENDMENT_EXPIRED");

        assertEquals(1, backend.declines.size());
        assertEquals("app-1", backend.declines.get(0).getApplicationId());
        assertEquals("RETURNED_FOR_AMENDMENT", backend.declines.get(0).getStage());
        assertEquals("AMENDMENT_EXPIRED", backend.declines.get(0).getReason());
    }

    @Test
    void backendRejectionThrowsARetryableFailure() {
        backend.rejectRequests = true;

        ApplicationFailure failure = assertThrows(ApplicationFailure.class,
                () -> activities.declineApplication("app-1", "RETURNED_FOR_AMENDMENT", "AMENDMENT_EXPIRED"));
        assertEquals("BACKEND_REJECTED", failure.getType());
        assertFalse(failure.isNonRetryable(), "Temporal must retry, not give up");

        assertThrows(ApplicationFailure.class,
                () -> activities.notifyStageOverdue("app-1", "RS1", "AWAITING_REVIEW", 0, "001", "NRB", 0L));
    }

    @Test
    void backendDownThrowsSoTemporalRetries() {
        server.shutdownNow();

        assertThrows(StatusRuntimeException.class,
                () -> activities.declineApplication("app-1", "RETURNED_FOR_AMENDMENT", "AMENDMENT_EXPIRED"));
    }
}
