package com.tracom.lango.worker.activities;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * Side effects the visit-cycle workflow needs. Apart from loading the policy, all
 * are callbacks to the Go backend, which stays the only writer to the database.
 */
@ActivityInterface
public interface VisitActivities {

    /** Reads the visit window and reminder timing from config, once per run (recorded in history). */
    @ActivityMethod(name = "LoadVisitPolicy")
    VisitPolicy loadVisitPolicy();

    /** Creates the visit (SCHEDULED) for this due date and returns its visit id. */
    @ActivityMethod(name = "ScheduleVisit")
    String scheduleVisit(String cycleId, String merchantId, String outletId, String dueDate, String windowEndDate);

    /** The window is about to close and the visit isn't done: the backend reminds the officer and supervisor. */
    @ActivityMethod(name = "RemindVisitDue")
    void remindVisitDue(String visitId, String cycleId, String windowEndDate);

    /** The window closed without a record, waiver or cancellation: the backend sets MISSED. */
    @ActivityMethod(name = "MarkVisitMissed")
    void markVisitMissed(String visitId, String cycleId, String dueDate);
}
