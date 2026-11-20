package com.tracom.lango.worker.activities;

/**
 * Visit timing from the lango.visits.* config. Loaded at the start of every run of a
 * visit-cycle workflow, so a config change applies from the next visit on.
 *
 * @param planningHorizonDays         days before its due date a visit is created (BA DFD-5 5.2), so the
 *                                    officer and merchant can agree a date before the window opens
 * @param windowDays                  days a visit stays open, starting on its due date
 * @param reminderDaysBeforeWindowEnd days before the window closes to remind; 0 = no reminder
 * @param zoneId                      time zone that due dates are in (a day starts at midnight there)
 */
public record VisitPolicy(int planningHorizonDays, int windowDays, int reminderDaysBeforeWindowEnd, String zoneId) {
}
