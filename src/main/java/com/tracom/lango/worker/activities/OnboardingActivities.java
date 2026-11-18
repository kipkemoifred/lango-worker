package com.tracom.lango.worker.activities;

import com.tracom.lango.proto.v1.ProvisionMerchantResponse;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * Side effects the onboarding workflow needs. Apart from loading the policy, all
 * are callbacks to the Go backend, which stays the only writer to the database.
 */
@ActivityInterface
public interface OnboardingActivities {

    /** Reads the stage deadlines and limits from config, once per workflow (recorded in history). */
    @ActivityMethod(name = "LoadOnboardingPolicy")
    OnboardingPolicy loadOnboardingPolicy();

    /** A review stage passed its deadline: tier 0 alerts the stage owners, 1+ management. */
    @ActivityMethod(name = "NotifyOnboardingStageOverdue")
    void notifyStageOverdue(
            String applicationId,
            String ticketNumber,
            String stage,
            int escalationTier,
            String branchCode,
            String regionCode,
            long overdueAtEpochMs);

    /** Declines the application when the worker decides it (amendment expired or too many returns). */
    @ActivityMethod(name = "DeclineOnboardingApplication")
    void declineApplication(String applicationId, String stage, String reason);

    /** Provisions in TMS / Core Banking via the backend and moves the application to ACTIVATED. */
    @ActivityMethod(name = "ProvisionMerchant")
    ProvisionMerchantResponse provisionMerchant(String applicationId, String ticketId, String approvedByActorId);
}
