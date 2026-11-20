package com.tracom.lango.worker.activities;

import java.util.concurrent.TimeUnit;

import org.jboss.logging.Logger;

import com.tracom.lango.proto.v1.RecordSlaBreachRequest;
import com.tracom.lango.proto.v1.RecordSlaBreachResponse;
import com.tracom.lango.proto.v1.RecordSlaWarningRequest;
import com.tracom.lango.proto.v1.RecordSlaWarningResponse;
import com.tracom.lango.proto.v1.SyncPausedSlaDurationRequest;
import com.tracom.lango.proto.v1.SyncPausedSlaDurationResponse;
import com.tracom.lango.proto.v1.TicketInternalCallbackServiceGrpc;

import io.quarkus.grpc.GrpcClient;
import io.temporal.failure.ApplicationFailure;

/**
 * Registered automatically on the default worker by quarkus-temporal (it finds
 * implementations of @ActivityInterface types and makes them CDI beans).
 *
 * Any exception thrown here is retried by Temporal using the workflow's
 * RetryOptions, so a backend outage delays escalations instead of losing them.
 */
public class TicketActivitiesImpl implements TicketActivities {

    private static final Logger LOG = Logger.getLogger(TicketActivitiesImpl.class);
    private static final long CALL_DEADLINE_SECONDS = 10; // below the 15s start-to-close timeout

    @GrpcClient("lango-go-backend")
    TicketInternalCallbackServiceGrpc.TicketInternalCallbackServiceBlockingStub goBackendClient;

    @Override
    public String recordAndNotifySlaEscalation(
            String ticketId,
            String ticketNumber,
            int escalationTier,
            String srmUserId,
            long breachedAtEpochMs,
            long totalPausedSeconds) {

        LOG.infof("SLA escalation: ticketNum=%s tier=%d", ticketNumber, escalationTier);

        RecordSlaBreachResponse response = goBackendClient
                .withDeadlineAfter(CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .recordSlaBreachAndEscalate(RecordSlaBreachRequest.newBuilder()
                        .setTicketId(ticketId)
                        .setTicketNumber(ticketNumber)
                        .setEscalationTier(escalationTier)
                        .setSrmUserId(srmUserId)
                        .setBreachedAtEpochMs(breachedAtEpochMs)
                        .setTotalPausedSeconds(totalPausedSeconds)
                        .build());

        if (!response.getSuccess()) {
            // Retryable: Temporal will call again with backoff.
            throw ApplicationFailure.newFailure(
                    "Backend did not record escalation tier " + escalationTier + " for " + ticketNumber,
                    "BACKEND_REJECTED");
        }
        return response.getCurrentTicketStatus();
    }

    @Override
    public void recordSlaWarning(
            String ticketId,
            String ticketNumber,
            String srmUserId,
            int warningPercent,
            long warnedAtEpochMs,
            long slaDueAtEpochMs) {

        LOG.infof("SLA warning: ticketNum=%s at %d%% of its SLA", ticketNumber, warningPercent);

        RecordSlaWarningResponse response = goBackendClient
                .withDeadlineAfter(CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .recordSlaWarning(RecordSlaWarningRequest.newBuilder()
                        .setTicketId(ticketId)
                        .setTicketNumber(ticketNumber)
                        .setSrmUserId(srmUserId)
                        .setWarningPercent(warningPercent)
                        .setWarnedAtEpochMs(warnedAtEpochMs)
                        .setSlaDueAtEpochMs(slaDueAtEpochMs)
                        .build());

        if (!response.getSuccess()) {
            throw ApplicationFailure.newFailure(
                    "Backend did not record the SLA warning for " + ticketNumber, "BACKEND_REJECTED");
        }
    }

    @Override
    public void syncPausedDurationToBackend(String ticketId, long totalPausedSeconds, String actorId) {
        LOG.infof("Syncing paused SLA duration (%ds) for ticketId=%s", totalPausedSeconds, ticketId);

        SyncPausedSlaDurationResponse response = goBackendClient
                .withDeadlineAfter(CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .syncPausedSlaDuration(SyncPausedSlaDurationRequest.newBuilder()
                        .setTicketId(ticketId)
                        .setTotalPausedSeconds(totalPausedSeconds)
                        .setUpdatedByActorId(actorId)
                        .build());

        if (!response.getSuccess()) {
            throw ApplicationFailure.newFailure(
                    "Backend did not store paused duration for " + ticketId, "BACKEND_REJECTED");
        }
    }
}
