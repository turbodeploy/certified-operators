package com.vmturbo.group.schedule;

import java.text.ParseException;
import java.util.Calendar;
import java.util.GregorianCalendar;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import net.fortuna.ical4j.model.Date;
import net.fortuna.ical4j.model.DateList;
import net.fortuna.ical4j.model.DateTime;
import net.fortuna.ical4j.model.Recur;
import net.fortuna.ical4j.model.TimeZone;
import net.fortuna.ical4j.model.TimeZoneRegistry;
import net.fortuna.ical4j.model.TimeZoneRegistryFactory;
import net.fortuna.ical4j.model.parameter.Value;

import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.Active;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.NextOccurrence;

/**
 * Helper class for use with {@link Schedule}.
 */
public class ScheduleUtils {

    private static final long MAX_END_DATE =
        new GregorianCalendar(2099, Calendar.DECEMBER, 31)
            .getTimeInMillis();
    private static final DateTime MAX_END_DATE_TIME = new DateTime(MAX_END_DATE);
    private static final TimeZoneRegistry TZ_REGISTRY = TimeZoneRegistryFactory.getInstance().createRegistry();

    private ScheduleUtils() {}

    /**
     * Calculate next schedule occurrence and remaining active time relative to the specified period
     * start time.
     *
     * @param schedule Schedule to use
     * @param periodStartTime Time against which to calculate next occurrence
     * @return Schedule with next occurrence populated, if any
     * @throws ParseException If recurrence pattern is invalid
     */
    public static Schedule calculateNextOccurrenceAndRemainingTimeActive(
        @Nonnull final Schedule.Builder schedule, final long periodStartTime) throws ParseException {
        final Date nextOccurrence = getFirstOcurrenceInPeriod(schedule, periodStartTime);
        if (nextOccurrence != null) {
            schedule.setNextOccurrence(
                NextOccurrence.newBuilder()
                    .setStartTime(nextOccurrence.getTime())
                    .build());
        }
        final long remainingTimeActive = calculateRemainingTimeActiveInMs(schedule, periodStartTime);
        if (remainingTimeActive > 0L) {
            schedule.setActive(
                Active.newBuilder()
                    .setRemainingActiveTimeMs(remainingTimeActive)
                    .build());
        }
        return schedule.build();
    }

    /**
     * If schedule is active, calculate remaining time it will be active.
     *
     * @param schedule Schedule to use
     * @param periodStartTime Time against which to calculate remaining time active
     * @return Remaining time active in milliseconds, or 0 is schedule is inactive at this time
     * @throws ParseException If recurrence pattern is invalid
     */
    public static long calculateRemainingTimeActiveInMs(@Nonnull final Schedule.Builder schedule,
                                                final long periodStartTime) throws ParseException {
        if (schedule.hasOneTime()) {
            if (periodStartTime < schedule.getStartTime()
                    || periodStartTime >= schedule.getEndTime()) {
                return 0L;
            }
        } else if (periodStartTime < schedule.getStartTime()
                || !schedule.hasPerpetual() && periodStartTime > schedule.getLastDate()) {
            return 0L;
        }


        final long eventDuration =  schedule.getEndTime() - schedule.getStartTime();

        // if there was an occurrence in the past slice of time equal of duration,
        // it means that the recurring event can still be active
        final Date recentOccurrence = getFirstOcurrenceInPeriod(schedule,
            periodStartTime - eventDuration);

        if (recentOccurrence != null && recentOccurrence.getTime() <= periodStartTime) {
            final long expectedEndTime = recentOccurrence.getTime() + eventDuration;
            final long endTime = Math.min(expectedEndTime,
                    schedule.getLastDate() > 0L ? schedule.getLastDate() : MAX_END_DATE);
            return endTime - periodStartTime;
        }
        return 0L;
    }

    @Nullable
    private static Date getFirstOcurrenceInPeriod(@Nonnull  final Schedule.Builder schedule,
              final long periodStartTime) throws ParseException {
        if (schedule.hasOneTime()) { //No recurrence pattern
            if (schedule.getStartTime() > periodStartTime && schedule.getStartTime() < MAX_END_DATE) {
                return new DateTime(schedule.getStartTime());
            }
        } else {
            /*
             * iCal4j is only accurate to the second (not milliseconds). Take the next
             * second to avoid returning a date in the past when the millis are omitted.
             * NOTE: this is copied over from OpsManager RecurringEventImpl
             */
            final long startTime = (long)((Math.ceil((periodStartTime + 1) / 1000.0)) * 1000);
            final long recurStartTime =
                    schedule.hasRecurrenceStart()
                            ? Math.max(
                                    schedule.getStartTime(),
                                    schedule.getRecurrenceStart().getRecurrenceStartTime())
                            : schedule.getStartTime();
            final TimeZone timeZone = TZ_REGISTRY.getTimeZone(schedule.getTimezoneId());
            final DateTime recurStartDateTime = new DateTime(
                new java.util.Date(Math.max(recurStartTime, startTime)), timeZone);
            final DateTime recurEndDateTime = schedule.hasPerpetual() ? MAX_END_DATE_TIME
                    : new DateTime(new java.util.Date(schedule.getLastDate()), timeZone);
            final Recur recur = new Recur(schedule.getRecurRule());
            final DateList dates = recur.getDates(new DateTime(new java.util.Date(schedule.getStartTime()),
                    timeZone), recurStartDateTime, recurEndDateTime, Value.DATE_TIME, 1);
            return dates.isEmpty() ? null : dates.get(0);
        }

        return null;
    }
}
