package com.tracom.lango.worker.activities;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import com.tracom.lango.proto.v1.DeclineOnboardingRequest;
import com.tracom.lango.proto.v1.OnboardingCallbackResponse;
import com.tracom.lango.proto.v1.OnboardingCallbackServiceGrpc;
import com.tracom.lango.proto.v1.OnboardingStageOverdueRequest;
import com.tracom.lango.proto.v1.ProvisionMerchantRequest;
import com.tracom.lango.proto.v1.ProvisionMerchantResponse;

import io.quarkiverse.temporal.TemporalActivity;
import io.quarkus.grpc.GrpcClient;
import io.temporal.failure.ApplicationFailure;

/**
 * Runs on the "onboarding" worker (lango-onboarding-tq), not the default ticket worker.
 *
 * Any exception thrown here is retried by Temporal using the workflow's
 * RetryOptions, so a backend outage delays onboarding instead of losing steps.
 */
@TemporalActivity(workers = "onboarding")
public class OnboardingActivitiesImpl implements OnboardingActivities {

    private static final Logger LOG = Logger.getLogger(OnboardingActivitiesImpl.class);
    private static final long CALL_DEADLINE_SECONDS = 10; // below the 15s start-to-close timeout

    @GrpcClient("lango-go-backend")
    OnboardingCallbackServiceGrpc.OnboardingCallbackServiceBlockingStub goBackendClient;

    @ConfigProperty(name = "lango.onboarding.review-deadline-hours")
    long reviewDeadlineHours;

    @ConfigProperty(name = "lango.onboarding.compliance-deadline-hours")
    long complianceDeadlineHours;

    @ConfigProperty(name = "lango.onboarding.ict-deadline-hours")
    long ictDeadlineHours;

    @ConfigProperty(name = "lango.onboarding.amendment-deadline-hours")
    long amendmentDeadlineHours;

    @ConfigProperty(name = "lango.onboarding.escalation-tier-hours")
    List<Long> escalationTierHours;

    @ConfigProperty(name = "lango.onboarding.max-amendment-cycles")
    int maxAmendmentCycles;

    @Override
    public OnboardingPolicy loadOnboardingPolicy() {
        return new OnboardingPolicy(reviewDeadlineHours, complianceDeadlineHours, ictDeadlineHours,
                amendmentDeadlineHours, List.copyOf(escalationTierHours), maxAmendmentCycles);
    }

    @Override
    public void notifyStageOverdue(
            String applicationId,
            String ticketNumber,
            String stage,
            int escalationTier,
            String branchCode,
            String regionCode,
            long overdueAtEpochMs) {

        LOG.infof("Onboarding stage overdue: application=%s stage=%s tier=%d", applicationId, stage, escalationTier);

        OnboardingCallbackResponse response = goBackendClient
                .withDeadlineAfter(CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .notifyOnboardingStageOverdue(OnboardingStageOverdueRequest.newBuilder()
                        .setApplicationId(applicationId)
                        .setTicketNumber(ticketNumber)
                        .setStage(stage)
                        .setEscalationTier(escalationTier)
                        .setBranchCode(branchCode)
                        .setRegionCode(regionCode)
                        .setOverdueAtEpochMs(overdueAtEpochMs)
                        .build());

        if (!response.getSuccess()) {
            // Retryable: Temporal will call again with backoff.
            throw ApplicationFailure.newFailure(
                    "Backend did not record overdue " + stage + " tier " + escalationTier + " for " + applicationId,
                    "BACKEND_REJECTED");
        }
    }

    @Override
    public void declineApplication(String applicationId, String stage, String reason) {
        LOG.infof("Declining onboarding application=%s at stage=%s: %s", applicationId, stage, reason);

        OnboardingCallbackResponse response = goBackendClient
                .withDeadlineAfter(CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .declineOnboardingApplication(DeclineOnboardingRequest.newBuilder()
                        .setApplicationId(applicationId)
                        .setStage(stage)
                        .setReason(reason)
                        .build());

        if (!response.getSuccess()) {
            throw ApplicationFailure.newFailure(
                    "Backend did not decline " + applicationId, "BACKEND_REJECTED");
        }
    }

    @Override
    public ProvisionMerchantResponse provisionMerchant(String applicationId, String ticketId, String approvedByActorId) {
        LOG.infof("Provisioning merchant for application=%s", applicationId);

        ProvisionMerchantResponse response = goBackendClient
                .withDeadlineAfter(CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .provisionMerchant(ProvisionMerchantRequest.newBuilder()
                        .setApplicationId(applicationId)
                        .setTicketId(ticketId)
                        .setApprovedByActorId(approvedByActorId)
                        .build());

        if (!response.getSuccess()) {
            throw ApplicationFailure.newFailure(
                    "Backend did not provision merchant for " + applicationId, "BACKEND_REJECTED");
        }
        return response;
    }
}
