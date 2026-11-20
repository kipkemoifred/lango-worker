package com.tracom.lango.worker.activities;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * Side effects the ticket workflow needs. Both are callbacks to the Go backend,
 * which stays the only writer to the operational database.
 */
@ActivityInterface
public interface TicketActivities {

    /** Records a breach (tier 0 = SRM, 1+ = management) and lets the backend send notifications. */
    @ActivityMethod(name = "RecordAndNotifySlaEscalation")
    String recordAndNotifySlaEscalation(
            String ticketId,
            String ticketNumber,
            int escalationTier,
            String srmUserId,
            long breachedAtEpochMs,
            long totalPausedSeconds);

    /** The SLA is close to breaching (warning percent of it used): the backend records it and warns the SRM. */
    @ActivityMethod(name = "RecordSlaWarning")
    void recordSlaWarning(
            String ticketId,
            String ticketNumber,
            String srmUserId,
            int warningPercent,
            long warnedAtEpochMs,
            long slaDueAtEpochMs);

    /** Stores the running total of paused SLA time on the ticket row. */
    @ActivityMethod(name = "SyncPausedDurationToBackend")
    void syncPausedDurationToBackend(String ticketId, long totalPausedSeconds, String actorId);
}
