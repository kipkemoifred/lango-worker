package com.tracom.lango.worker.activities;

import java.util.concurrent.TimeUnit;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import com.tracom.lango.proto.v1.MarkVisitMissedRequest;
import com.tracom.lango.proto.v1.RemindVisitDueRequest;
import com.tracom.lango.proto.v1.ScheduleVisitRequest;
import com.tracom.lango.proto.v1.ScheduleVisitResponse;
import com.tracom.lango.proto.v1.VisitCallbackResponse;
import com.tracom.lango.proto.v1.VisitCallbackServiceGrpc;

import io.quarkiverse.temporal.TemporalActivity;
import io.quarkus.grpc.GrpcClient;
import io.temporal.failure.ApplicationFailure;

/**
 * Runs on the "visits" worker (lango-visits-tq).
 *
 * Any exception thrown here is retried by Temporal using the workflow's
 * RetryOptions, so a backend outage delays visits instead of losing them.
 */
@TemporalActivity(workers = "visits")
public class VisitActivitiesImpl implements VisitActivities {

    private static final Logger LOG = Logger.getLogger(VisitActivitiesImpl.class);
    private static final long CALL_DEADLINE_SECONDS = 10; // below the 15s start-to-close timeout

    @GrpcClient("lango-go-backend")
    VisitCallbackServiceGrpc.VisitCallbackServiceBlockingStub goBackendClient;

    @ConfigProperty(name = "lango.visits.planning-horizon-days")
    int planningHorizonDays;

    @ConfigProperty(name = "lango.visits.window-days")
    int windowDays;

    @ConfigProperty(name = "lango.visits.reminder-days-before-window-end")
    int reminderDaysBeforeWindowEnd;

    @ConfigProperty(name = "lango.visits.zone")
    String zoneId;

    @Override
    public VisitPolicy loadVisitPolicy() {
        return new VisitPolicy(planningHorizonDays, windowDays, reminderDaysBeforeWindowEnd, zoneId);
    }

    @Override
    public String scheduleVisit(String cycleId, String merchantId, String outletId, String dueDate, String windowEndDate) {
        LOG.infof("Scheduling visit: cycle=%s due=%s window ends %s", cycleId, dueDate, windowEndDate);

        ScheduleVisitResponse response = goBackendClient
                .withDeadlineAfter(CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .scheduleVisit(ScheduleVisitRequest.newBuilder()
                        .setCycleId(cycleId)
                        .setMerchantId(merchantId)
                        .setOutletId(outletId)
                        .setDueDate(dueDate)
                        .setWindowEndDate(windowEndDate)
                        .build());

        if (!response.getSuccess() || response.getVisitId().isEmpty()) {
            // Retryable: Temporal will call again with backoff.
            throw ApplicationFailure.newFailure(
                    "Backend did not schedule the " + dueDate + " visit for cycle " + cycleId, "BACKEND_REJECTED");
        }
        return response.getVisitId();
    }

    @Override
    public void remindVisitDue(String visitId, String cycleId, String windowEndDate) {
        LOG.infof("Visit due reminder: visit=%s cycle=%s window ends %s", visitId, cycleId, windowEndDate);

        VisitCallbackResponse response = goBackendClient
                .withDeadlineAfter(CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .remindVisitDue(RemindVisitDueRequest.newBuilder()
                        .setVisitId(visitId)
                        .setCycleId(cycleId)
                        .setWindowEndDate(windowEndDate)
                        .build());

        if (!response.getSuccess()) {
            throw ApplicationFailure.newFailure("Backend did not record reminder for visit " + visitId, "BACKEND_REJECTED");
        }
    }

    @Override
    public void markVisitMissed(String visitId, String cycleId, String dueDate) {
        LOG.infof("Visit missed: visit=%s cycle=%s due=%s", visitId, cycleId, dueDate);

        VisitCallbackResponse response = goBackendClient
                .withDeadlineAfter(CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .markVisitMissed(MarkVisitMissedRequest.newBuilder()
                        .setVisitId(visitId)
                        .setCycleId(cycleId)
                        .setDueDate(dueDate)
                        .build());

        if (!response.getSuccess()) {
            throw ApplicationFailure.newFailure("Backend did not mark visit " + visitId + " missed", "BACKEND_REJECTED");
        }
    }
}
