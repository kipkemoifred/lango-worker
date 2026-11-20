package com.tracom.lango.worker.activities;

import java.util.List;

/**
 * Deadlines and limits for one onboarding workflow, from the lango.onboarding.* config.
 * Loaded once when the workflow starts, so a config change only affects new applications.
 *
 * @param reviewDeadlineHours     AWAITING_REVIEW (branch supervisor) before the first alert
 * @param complianceDeadlineHours COMPLIANCE_REVIEW (compliance officer) before the first alert
 * @param inspectionDeadlineHours SITE_INSPECTION (sales agent captures it) before the first alert
 * @param inspectionApprovalDeadlineHours INSPECTION_APPROVAL (branch supervisor) before the first alert
 * @param ictDeadlineHours        ICT_PROVISIONING (ICT officer) before the first alert
 * @param amendmentDeadlineHours  time the sales agent has to resubmit before the application is declined
 * @param escalationTierHours     gap in hours before management tier 1..N after the first alert
 * @param maxAmendmentCycles      returns allowed; one more return declines the application
 */
public record OnboardingPolicy(
        long reviewDeadlineHours,
        long complianceDeadlineHours,
        long inspectionDeadlineHours,
        long inspectionApprovalDeadlineHours,
        long ictDeadlineHours,
        long amendmentDeadlineHours,
        List<Long> escalationTierHours,
        int maxAmendmentCycles) {
}
