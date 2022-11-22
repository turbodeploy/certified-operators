package com.vmturbo.group.schedule;

import java.text.ParseException;
import java.util.Calendar;
import java.util.GregorianCalendar;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import net.fortuna.ical4j.model.DateList;
import net.fortuna.ical4j.model.DateTime;
import net.fortuna.ical4j.model.Recur;
import net.fortuna.ical4j.model.TimeZone;
import net.fortuna.ical4j.model.TimeZoneRegistry;
import net.fortuna.ical4j.model.TimeZoneRegistryFactory;
import net.fortuna.ical4j.model.parameter.Value;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dmfs.rfc5545.recur.InvalidRecurrenceRuleException;
import org.dmfs.rfc5545.recur.RecurrenceRule;
import org.dmfs.rfc5545.recur.RecurrenceRule.RfcMode;
import org.dmfs.rfc5545.recur.RecurrenceRuleIterator;

import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.Active;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.NextOccurrence;

/**
 * Helper class for use with {@link Schedule}.
 */
public class ScheduleUtils {

    private static final long MILLISECONDS_IN_A_DAY = 24L * 60L * 60L * 1000L;
    private static final long TWENTY_FIVE_HOURS = 25L * 60L * 60L * 1000L;
    private static final long MAX_END_DATE =
        new GregorianCalendar(2099, Calendar.DECEMBER, 31)
            .getTimeInMillis();
    private static final DateTime MAX_END_DATE_TIME = new DateTime(MAX_END_DATE);
    private static final TimeZoneRegistry TZ_REGISTRY = TimeZoneRegistryFactory.getInstance().createRegistry();
    private static final Logger logger = LogManager.getLogger();

    private final boolean useLibRecur;

    /**
     * Creates an instance of ScheduleUtils for calculating recurrences.
     *
     * @param useLibRecur true if this utility should use the lib-recur library.
     *                    False if this utility should use the iCal4j library.
     */
    public ScheduleUtils(boolean useLibRecur) {
        this.useLibRecur = useLibRecur;
    }

    /**
     * Calculate next schedule occurrence and remaining active time relative to the specified period
     * start time.
     *
     * @param schedule Schedule to use
     * @param periodStartTime Time against which to calculate next occurrence
     * @return Schedule with next occurrence populated, if any
     * @throws ParseException If recurrence pattern is invalid
     */
    public Schedule calculateNextOccurrenceAndRemainingTimeActive(
        @Nonnull final Schedule.Builder schedule, final long periodStartTime)
            throws ParseException, InvalidRecurrenceRuleException {
        final Long nextOccurrence;
        if (useLibRecur) {
            nextOccurrence = getFirstOccurrenceInPeriodUsingLibRecur(schedule, periodStartTime);
        } else {
            nextOccurrence = getFirstOccurrenceInPeriodUsingICal4J(schedule, periodStartTime);
        }
        if (nextOccurrence != null) {
            schedule.setNextOccurrence(
                NextOccurrence.newBuilder()
                    .setStartTime(nextOccurrence)
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
    public long calculateRemainingTimeActiveInMs(@Nonnull final Schedule.Builder schedule,
                                                final long periodStartTime)
            throws ParseException, InvalidRecurrenceRuleException {
        if (schedule.hasOneTime()) {
            if (periodStartTime < schedule.getStartTime()
                    || periodStartTime >= schedule.getEndTime()) {
                return 0L;
            }
        } else if (periodStartTime < schedule.getStartTime()
                || !schedule.hasPerpetual() && periodStartTime > schedule.getLastDate()) {
            return 0L;
        }

        final long userSelectedDuration = schedule.getEndTime() - schedule.getStartTime();
        final long eventDuration;
        if (userSelectedDuration == MILLISECONDS_IN_A_DAY
                && (   "FREQ=DAILY;INTERVAL=1;".equals(schedule.getRecurRule())
                    || "INTERVAL=1;FREQ=DAILY;".equals(schedule.getRecurRule())
                )
        ) {
            // Use timezone to calculate an entire day to overcome OM-92616 where the
            // 24 hours was not sufficient to cover the entire day as day light savings
            // was ending. The day when day light savings ends has 25 hours where 1am
            // occurs twice!
            //
            // For example:
            // 2022-11-06T01:59-05:00[America/Chicago]    2022-11-06T06:59Z[UTC]
            // 2022-11-06T01:00-06:00[America/Chicago]    2022-11-06T07:00Z[UTC]
            // 2022-11-06T01:59-06:00[America/Chicago]    2022-11-06T07:59Z[UTC]
            // 2022-11-06T02:00-06:00[America/Chicago]    2022-11-06T08:00Z[UTC]
            //
            // It's critical that there are no gaps, not even one hour because customers use a
            // 'All day, every day' schedule to ensure disable actions since schedule policies have
            // the highest priority when resolving conflicts.
            eventDuration = TWENTY_FIVE_HOURS;
        } else {
            eventDuration = userSelectedDuration;
        }


        // if there was an occurrence in the past slice of time equal of duration,
        // it means that the recurring event can still be active
        final Long recentOccurrence;
        if (useLibRecur) {
            recentOccurrence = getFirstOccurrenceInPeriodUsingLibRecur(schedule,
                    periodStartTime - eventDuration);
        } else {
            recentOccurrence = getFirstOccurrenceInPeriodUsingICal4J(schedule,
                    periodStartTime - eventDuration);
        }

        if (recentOccurrence != null && recentOccurrence <= periodStartTime) {
            final long expectedEndTime = recentOccurrence + eventDuration;
            final long endTime = Math.min(expectedEndTime,
                    schedule.getLastDate() > 0L ? schedule.getLastDate() : MAX_END_DATE);
            return endTime - periodStartTime;
        }
        return 0L;
    }

    @Nullable
    private static Long getFirstOccurrenceInPeriodUsingICal4J(@Nonnull  final Schedule.Builder schedule,
              final long periodStartTime) throws ParseException {
        if (schedule.hasOneTime()) { //No recurrence pattern
            if (schedule.getStartTime() > periodStartTime && schedule.getStartTime() < MAX_END_DATE) {
                return schedule.getStartTime();
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

            return dates.isEmpty() ? null : dates.get(0).getTime();
        }

        return null;
    }

    @Nullable
    private static Long getFirstOccurrenceInPeriodUsingLibRecur(@Nonnull  final Schedule.Builder schedule,
            final long refTime) throws InvalidRecurrenceRuleException {
        if (schedule.hasOneTime()) { //No recurrence pattern
            if (schedule.getStartTime() > refTime && schedule.getStartTime() < MAX_END_DATE) {
                return schedule.getStartTime();
            }
        } else {
            final RecurrenceRule rr = new RecurrenceRule(schedule.getRecurRule(), RfcMode.RFC2445_STRICT);

            final long iteratorInstantSeed = schedule.getStartTime();
            final long occurrenceStartInstant;
            if (schedule.hasRecurrenceStart()) {
                occurrenceStartInstant = schedule.getRecurrenceStart().getRecurrenceStartTime();
            } else {
                occurrenceStartInstant = iteratorInstantSeed;
            }
            RecurrenceRuleIterator iterator = rr.iterator(iteratorInstantSeed, java.util.TimeZone.getTimeZone(schedule.getTimezoneId()));
            while (iterator.hasNext()) {
                long next = iterator.nextMillis();
                if (!schedule.hasPerpetual() && next > schedule.getLastDate()) {
                    logger.trace("no next occurrence for schedule.getId()={}"
                                    + " schedule.getDisplayName()={}"
                                    + " schedule.hasPerpetual()={}"
                                    + " because next={} exceeds schedule.getLastDate()={}",
                            schedule.getId(),
                            schedule.getDisplayName(),
                            schedule.hasPerpetual(),
                            next,
                            schedule.getLastDate());
                    return null;
                }
                if (next >= occurrenceStartInstant && next > refTime) {
                    return next;
                }
            }
        }

        logger.trace("no next occurrence for"
                + " schedule.getId()={}"
                + " schedule.getDisplayName()={}",
                schedule.getId(),
                schedule.getDisplayName());
        return null;
    }
}
