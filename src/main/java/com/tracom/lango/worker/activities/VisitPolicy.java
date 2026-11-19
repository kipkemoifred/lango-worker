package com.tracom.lango.worker.activities;

/**
 * Visit timing from the lango.visits.* config. Loaded at the start of every run of a
 * visit-cycle workflow, so a config change applies from the next visit on.
 *
 * @param windowDays                  days a visit stays open, starting on its due date
 * @param reminderDaysBeforeWindowEnd days before the window closes to remind; 0 = no reminder
 * @param zoneId                      time zone that due dates are in (a day starts at midnight there)
 */
public record VisitPolicy(int windowDays, int reminderDaysBeforeWindowEnd, String zoneId) {
}
