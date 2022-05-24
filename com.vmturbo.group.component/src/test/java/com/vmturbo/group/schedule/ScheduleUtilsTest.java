package com.vmturbo.group.schedule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.dmfs.rfc5545.recur.InvalidRecurrenceRuleException;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.OneTime;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.Perpetual;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.RecurrenceStart;

/**
 * Unit tests for {@link ScheduleUtils}.
 *
 * <p>Summary of daily test cases explored and their status:</p>
 * <pre>
 * DONE               Before Start
 * DONE               Exactly First Occurrence
 * DONE               Within First Occurrence
 * DONE               Exactly End of First Occurrence
 * DONE               Just after First Occurrence
 * DONE               Just before Second Occurrence
 * DONE               Exactly at Second Occurrence
 * DONE               Within Second Occurrence
 * DONE               Exactly End of Second Occurrence
 * DONE               (America/Chicago) start time is fall daylight savings ref time is spring
 * DONE               (America/Chicago) start time is spring daylight savings ref time is fall
 * DONE               (America/Chicago) start time is spring daylight savings ref time is spring of next year
 * DONE               (America/Chicago) start time is fall daylight savings ref time is fall of next year
 * DONE               (Europe/Paris) start time is fall daylight savings ref time is spring
 * DONE               (Europe/Paris) start time is spring daylight savings ref time is fall
 * DONE               (Europe/Paris) start time is spring daylight savings ref time is spring of next year
 * DONE               (Europe/Paris) start time is fall daylight savings ref time is fall of next year
 * DONE               One of the schedule's occurrence's start time is 2am (which is actually 1am in the new timezone),
 *                    exactly when daylight transition falls back
 *                    Customer should use 1:59:59am instead of 2am in that case.
 * DONE               One of the schedule's occurrence's start time is 2:01am (which is actually 1:01am in the new timezone),
 *                    just after the daylight transition falls back
 *                    Customer should use 1:59:59am instead of 2am in that case.
 * DONE               One of the schedule's occurrence's start time is 1:59am
 *                    just before when daylight transition falls back
 * BROKEN (OM-84347)  The ref time is first hour of 1am to 2am in daylight savings time CDT (-05:00)
 *                    in this case we would expect 2 hours because there is still another 1am to 2am
 * DONE               The ref time is second hour of 1am to 2am in daylight savings time CST (-06:00)
 *                    in this case we would expect 1 hours because the first 1am to 2am hour already
 *                    passed.
 * </pre>
 */
public class ScheduleUtilsTest {

    private static final long SCHEDULE_ID = 1L;
    private static final long START_TIME = Instant.parse("2010-01-10T09:30:00Z").toEpochMilli();
    private static final long END_TIME = Instant.parse("2010-01-10T15:00:00Z").toEpochMilli();
    private static final long RECURRENCE_START_TIME = Instant.parse("2010-01-17T09:30:00Z").toEpochMilli();
    private static final long LAST_DATE = Instant.parse("2010-12-31T23:59:59Z").toEpochMilli();
    private static  final Schedule SCHEDULE = Schedule.newBuilder()
        .setId(SCHEDULE_ID)
        .setStartTime(START_TIME)
        .setEndTime(END_TIME)
        .setTimezoneId(ZoneId.of("UTC").getId())
        .build();

    /** Expected exceptions to test against. */
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /**
     * Test one time schedule.
     *
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testNextOccurrenceOneTime() throws Exception {
        testNextOccurrenceOneTime(new ScheduleUtils(false));
        testNextOccurrenceOneTime(new ScheduleUtils(true));
    }

    private void testNextOccurrenceOneTime(ScheduleUtils scheduleUtils) throws Exception {
        final Schedule oneTimeSchedule = SCHEDULE.toBuilder().setOneTime(OneTime.newBuilder().build())
            .build();
        // before schedule starts
        Schedule schedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            oneTimeSchedule.toBuilder(), Instant.parse("2010-01-01T09:30:00Z").toEpochMilli());
        assertTrue(schedule.hasNextOccurrence());
        assertEquals(START_TIME, schedule.getNextOccurrence().getStartTime());

        schedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            oneTimeSchedule.toBuilder(), SCHEDULE.getStartTime());
        assertFalse(schedule.hasNextOccurrence());

        schedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            oneTimeSchedule.toBuilder(), Instant.parse("2012-01-01T09:30:00Z").toEpochMilli());
        assertFalse(schedule.hasNextOccurrence());
    }

    /**
     * Test recurring daily perpetual schedule, every 1 day.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testNextOccurrenceRecurDailyOneDayPerpetual() throws Exception {
        testNextOccurrenceRecurDailyOneDayPerpetual(new ScheduleUtils(false));
        testNextOccurrenceRecurDailyOneDayPerpetual(new ScheduleUtils(true));
    }

    private void testNextOccurrenceRecurDailyOneDayPerpetual(ScheduleUtils scheduleUtils) throws Exception {
        // Before Start
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The ref time is before the schedule begins, as a result the next"
                        + " occurrence should be the first occurrence of the schedule.")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withStartTime("2010-01-10T09:30:00Z")
                .withRefTime(  "2010-01-10T09:00:00Z")
                .withExpected( "2010-01-10T09:30:00Z")
                .check();
        // Exactly First Occurrence
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The refTime is at exactly the beginning of first occurrence for this schedule"
                        + " so the next occurrence should be in one day")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withStartTime("2010-01-10T09:30:00Z")
                .withRefTime(  "2010-01-10T09:30:00Z")
                .withExpected( "2010-01-11T09:30:00Z")
                .check();
        // Within First Occurrence
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The refTime is in the middle of the first occurrence for this schedule"
                        + " so the next occurrence should be in one day")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withStartTime("2010-01-10T09:30:00Z")
                .withRefTime(  "2010-01-10T10:00:00Z")
                .withExpected( "2010-01-11T09:30:00Z")
                .check();
        // Exactly End of First Occurrence
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The refTime is exactly at the end of the first occurrence for this schedule"
                        + " so the next occurrence should be in one day")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withStartTime("2010-01-10T09:30:00Z")
                .withEndTime(  "2010-01-10T15:00:00Z")
                .withRefTime(  "2010-01-10T15:00:00Z")
                .withExpected( "2010-01-11T09:30:00Z")
                .check();
        //Just after First Occurrence
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The refTime is just after the first occurrence for this schedule"
                        + " so the next occurrence should be in one day")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withStartTime("2010-01-10T09:30:00Z")
                .withEndTime(  "2010-01-10T15:00:00Z")
                .withRefTime(  "2010-01-10T15:00:01Z")
                .withExpected( "2010-01-11T09:30:00Z")
                .check();
        //Just before Second Occurrence
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The refTime is just before the second occurrence for this schedule"
                        + " so the next occurrence should be the second occurrence")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withStartTime("2010-01-10T09:30:00Z")
                .withEndTime(  "2010-01-10T15:00:00Z")
                .withRefTime(  "2010-01-11T09:29:59Z")
                .withExpected( "2010-01-11T09:30:00Z")
                .check();
        //Exactly at Second Occurrence
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The refTime is just at the second occurrence for this schedule"
                        + " so the next occurrence should be the third occurrence")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withStartTime("2010-01-10T09:30:00Z")
                .withEndTime(  "2010-01-10T15:00:00Z")
                .withRefTime(  "2010-01-11T09:30:00Z")
                .withExpected( "2010-01-12T09:30:00Z")
                .check();
        // Within Second Occurrence
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The refTime inside the second occurrence for this schedule"
                        + " so the next occurrence should be the third occurrence")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withStartTime("2010-01-10T09:30:00Z")
                .withEndTime(  "2010-01-10T15:00:00Z")
                .withRefTime(  "2010-01-11T10:30:00Z")
                .withExpected( "2010-01-12T09:30:00Z")
                .check();
        //Exactly End of Second Occurrence
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The refTime is just at the second occurrence for this schedule"
                        + " so the next occurrence should be the third occurrence")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withStartTime("2010-01-10T09:30:00Z")
                .withEndTime(  "2010-01-10T15:00:00Z")
                .withRefTime(  "2010-01-11T15:00:00Z")
                .withExpected( "2010-01-12T09:30:00Z")
                .check();

        //(America/Chicago) start time is fall daylight savings ref time is spring
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The startTime is in the fall the ref time is in the spring"
                        + " so the next occurrence's time component should not be impacted by"
                        + " daylight savings")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withTimezone("America/Chicago")
                .withStartTime("2020-11-28T03:00:00-06:00[America/Chicago]")
                .withEndTime(  "2020-11-28T05:00:00-06:00[America/Chicago]")
                .withRefTime(  "2021-05-16T14:42:02-05:00[America/Chicago]")
                .withExpected( "2021-05-17T03:00:00-05:00[America/Chicago]")
                .check();
        //(America/Chicago) start time is spring daylight savings ref time is fall
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The startTime is in the spring the ref time is in the fall"
                        + " so the next occurrence's time component should not be impacted by"
                        + " daylight savings")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withTimezone("America/Chicago")
                .withStartTime("2020-05-16T03:00:00-05:00[America/Chicago]")
                .withEndTime(  "2020-05-16T05:00:00-05:00[America/Chicago]")
                .withRefTime(  "2020-11-28T14:42:02-06:00[America/Chicago]")
                .withExpected( "2020-11-29T03:00:00-06:00[America/Chicago]")
                .check();
        //(America/Chicago) start time is spring daylight savings ref time is spring of next year (OM-82886)
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The startTime is in the fall the ref time is in the spring"
                        + " so the next occurrence's time component should not be impacted by"
                        + " daylight savings")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withTimezone("America/Chicago")
                .withStartTime("2020-07-28T03:00:00-05:00[America/Chicago]")
                .withEndTime(  "2020-07-28T05:00:00-05:00[America/Chicago]")
                .withRefTime(  "2021-05-16T14:42:02-05:00[America/Chicago]")
                .withExpected( "2021-05-17T03:00:00-05:00[America/Chicago]")
                .check();
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The startTime is in the fall the ref time is in the spring"
                        + " so the next occurrence's time component should not be impacted by"
                        + " daylight savings")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withTimezone("America/Chicago")
                .withStartTime("2020-07-28T03:00:00-05:00[America/Chicago]")
                .withEndTime(  "2020-07-28T05:00:00-05:00[America/Chicago]")
                .withRefTime(  "2022-05-16T14:42:02-05:00[America/Chicago]")
                .withExpected( "2022-05-17T03:00:00-05:00[America/Chicago]")
                .check();
        //(America/Chicago) start time is fall daylight savings ref time is fall of next year
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The startTime is in the fall the ref time is in the fall"
                        + " so the next occurrence's time component should not be impacted by"
                        + " daylight savings")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withTimezone("America/Chicago")
                .withStartTime("2020-11-28T03:00:00-06:00[America/Chicago]")
                .withEndTime(  "2020-11-28T05:00:00-06:00[America/Chicago]")
                .withRefTime(  "2021-11-16T14:42:02-06:00[America/Chicago]")
                .withExpected( "2021-11-17T03:00:00-06:00[America/Chicago]")
                .check();
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The startTime is in the fall the ref time is in the fall"
                        + " so the next occurrence's time component should not be impacted by"
                        + " daylight savings")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withTimezone("America/Chicago")
                .withStartTime("2020-11-28T03:00:00-06:00[America/Chicago]")
                .withEndTime(  "2020-11-28T05:00:00-06:00[America/Chicago]")
                .withRefTime(  "2022-11-16T14:42:02-06:00[America/Chicago]")
                .withExpected( "2022-11-17T03:00:00-06:00[America/Chicago]")
                .check();

        //(Europe/Paris) start time is fall daylight savings ref time is spring
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The startTime is in the fall the ref time is in the spring"
                        + " so the next occurrence's time component should not be impacted by"
                        + " daylight savings")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withTimezone("Europe/Paris")
                .withStartTime("2020-11-28T03:00:00+01:00[Europe/Paris]")
                .withEndTime(  "2020-11-28T05:00:00+01:00[Europe/Paris]")
                .withRefTime(  "2021-05-16T14:42:02+02:00[Europe/Paris]")
                .withExpected( "2021-05-17T03:00:00+02:00[Europe/Paris]")
                .check();
        //(Europe/Paris) start time is spring daylight savings ref time is fall
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The startTime is in the spring the ref time is in the fall"
                        + " so the next occurrence's time component should not be impacted by"
                        + " daylight savings")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withTimezone("America/Chicago")
                .withStartTime("2020-05-16T03:00:00+02:00[Europe/Paris]")
                .withEndTime(  "2020-05-16T05:00:00+02:00[Europe/Paris]")
                .withRefTime(  "2020-11-28T14:42:02+01:00[Europe/Paris]")
                .withExpected( "2020-11-29T03:00:00+01:00[Europe/Paris]")
                .check();
        //(Europe/Paris) start time is spring daylight savings ref time is spring of next year (OM-82886)
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The startTime is in the fall the ref time is in the spring"
                        + " so the next occurrence's time component should not be impacted by"
                        + " daylight savings")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withTimezone("Europe/Paris")
                .withStartTime("2020-07-28T03:00:00+02:00[Europe/Paris]")
                .withEndTime(  "2020-07-28T05:00:00+02:00[Europe/Paris]")
                .withRefTime(  "2021-05-16T14:42:02+02:00[Europe/Paris]")
                .withExpected( "2021-05-17T03:00:00+02:00[Europe/Paris]")
                .check();
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The startTime is in the fall the ref time is in the spring"
                        + " so the next occurrence's time component should not be impacted by"
                        + " daylight savings")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withTimezone("Europe/Paris")
                .withStartTime("2020-07-28T03:00:00+02:00[Europe/Paris]")
                .withEndTime(  "2020-07-28T05:00:00+02:00[Europe/Paris]")
                .withRefTime(  "2022-05-16T14:42:02+02:00[Europe/Paris]")
                .withExpected( "2022-05-17T03:00:00+02:00[Europe/Paris]")
                .check();
        //(Europe/Paris) start time is fall daylight savings ref time is fall of next year
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The startTime is in the fall the ref time is in the fall"
                        + " so the next occurrence's time component should not be impacted by"
                        + " daylight savings")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withTimezone("Europe/Paris")
                .withStartTime("2020-11-28T03:00:00+01:00[Europe/Paris]")
                .withEndTime(  "2020-11-28T05:00:00+01:00[Europe/Paris]")
                .withRefTime(  "2021-11-16T14:42:02+01:00[Europe/Paris]")
                .withExpected( "2021-11-17T03:00:00+01:00[Europe/Paris]")
                .check();
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The startTime is in the fall the ref time is in the fall"
                        + " so the next occurrence's time component should not be impacted by"
                        + " daylight savings")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withTimezone("Europe/Paris")
                .withStartTime("2020-11-28T03:00:00+01:00[Europe/Paris]")
                .withEndTime(  "2020-11-28T05:00:00+01:00[Europe/Paris]")
                .withRefTime(  "2022-11-16T14:42:02+01:00[Europe/Paris]")
                .withExpected( "2022-11-17T03:00:00+01:00[Europe/Paris]")
                .check();
    }

    /**
     * Start time is in fall daylight savings ref time is spring but daylight transitions at 2am to 3am
     * one of the schedule occurrence's is exactly 2:00am (exactly at the jump) to 3am.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testScheduleOccurrenceExactlyDaylightTransition() throws Exception {
        //(America/Chicago) start time is fall daylight savings ref time is spring
        //                  but daylight transitions at 2am to 3am
        //                  one of the schedule occurrence's is exactly 2am to 3am
        new RecurrenceTestCase()
                .withScheduleUtils(new ScheduleUtils(true)) // ical4j does not handle this case
                .withExplanation("The startTime is in the fall the ref time is in the spring"
                        + " so the next occurrence's time component should not be impacted by"
                        + " daylight savings")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withTimezone("America/Chicago")
                .withStartTime("2020-11-28T02:00:00-06:00[America/Chicago]")
                .withEndTime(  "2020-11-28T03:00:00-06:00[America/Chicago]")
                .withRefTime(  "2021-05-16T14:42:02-05:00[America/Chicago]")
                .withExpected( "2021-05-17T02:00:00-05:00[America/Chicago]")
                .check();
    }

    /**
     * Start time is in fall daylight savings ref time is spring but daylight transitions at 2am to 3am
     * one of the schedule occurrence's is just after 2:00am (within the jump) to 3am.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testScheduleOccurrenceJustAfterStartOfDaylightTransition() throws Exception {
        new RecurrenceTestCase()
                .withScheduleUtils(new ScheduleUtils(true)) // ical4j does not handle this case
                .withExplanation("Start time is in fall daylight savings ref time is spring but daylight transitions at 2am to 3am"
                        + " one of the schedule occurrence's is exactly 2:00am (exactly at the jump) to 3am.")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withTimezone("America/Chicago")
                .withStartTime("2020-11-28T02:01:00-06:00[America/Chicago]")
                .withEndTime(  "2020-11-28T03:00:00-06:00[America/Chicago]")
                .withRefTime(  "2021-05-16T14:42:02-05:00[America/Chicago]")
                .withExpected( "2021-05-17T02:01:00-05:00[America/Chicago]")
                .check();
    }

    /**
     * Start time is in fall daylight savings ref time is spring but daylight transitions at 2am to 3am
     * one of the schedule occurrence's is exactly 1:59am (just before the jump) to 3am.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testScheduleOccurrenceJustBeforeStartOfDaylightTransition() throws Exception {
        testScheduleOccurrenceJustBeforeStartOfDaylightTransition(new ScheduleUtils(false));
        testScheduleOccurrenceJustBeforeStartOfDaylightTransition(new ScheduleUtils(true));
    }

    private void testScheduleOccurrenceJustBeforeStartOfDaylightTransition(ScheduleUtils scheduleUtils) throws Exception {
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("Start time is in fall daylight savings ref time is spring but daylight transitions at 2am to 3am"
                        + " one of the schedule occurrence's is exactly 1:59am (just before the jump) to 3am")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withTimezone("America/Chicago")
                .withStartTime("2020-11-28T01:59:00-06:00[America/Chicago]")
                .withEndTime(  "2020-11-28T03:00:00-06:00[America/Chicago]")
                .withRefTime(  "2021-05-16T14:42:02-05:00[America/Chicago]")
                .withExpected( "2021-05-17T01:59:00-05:00[America/Chicago]")
                .check();
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The startTime is in the fall the ref time is in the spring"
                        + " so the next occurrence's time component should not be impacted by"
                        + " daylight savings")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withTimezone("America/Chicago")
                .withStartTime("2020-11-28T01:59:00-06:00[America/Chicago]")
                .withEndTime(  "2020-11-28T02:59:00-06:00[America/Chicago]")
                .withRefTime(  "2021-05-16T14:42:02-05:00[America/Chicago]")
                .withExpected( "2021-05-17T01:59:00-05:00[America/Chicago]")
                .check();
    }

    /**
     * A schedule from 1am to 2am, during the first hour in the fall during daylight savings transition should
     * have 1 hour left.
     *
     * <p>OM-84347: This test fails. We do not handle the transition during daylight savings</p>
     *
     * @throws Exception should not be thrown.
     */
    @Ignore
    @Test
    public void testFirstHourWithinDaylightTransitionSpringToFall() throws Exception {
        testFirstHourWithinDaylightTransitionSpringToFall(new ScheduleUtils(false));
        testFirstHourWithinDaylightTransitionSpringToFall(new ScheduleUtils(true));
    }

    private void testFirstHourWithinDaylightTransitionSpringToFall(ScheduleUtils scheduleUtils) throws Exception {
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The startTime is in the fall the ref time is in the spring"
                        + " so the next occurrence's time component should not be impacted by"
                        + " daylight savings")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withTimezone("America/Chicago")
                .withStartTime("2020-05-28T01:00:00-05:00[America/Chicago]")
                .withEndTime(  "2020-05-28T02:00:00-05:00[America/Chicago]")
                .withRefTime(  "2021-11-16T01:00:00-05:00[America/Chicago]") // this is the first hour during the daylight transition
                .withExpected( "2021-11-17T01:00:00-06:00[America/Chicago]")
                .withExpectedActiveDurationMsec(TimeUnit.HOURS.toMillis(2))
                .check();
    }

    /**
     * A schedule from 1am to 2am, during the second hour in the fall during daylight savings transition should
     * have 1 hour left.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testSecondHourWithinDaylightTransitionSpringToFall() throws Exception {
        testSecondHourWithinDaylightTransitionSpringToFall(new ScheduleUtils(false));
        testSecondHourWithinDaylightTransitionSpringToFall(new ScheduleUtils(true));
    }

    private void testSecondHourWithinDaylightTransitionSpringToFall(ScheduleUtils scheduleUtils) throws Exception {
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The startTime is in the fall the ref time is in the spring"
                        + " so the next occurrence's time component should not be impacted by"
                        + " daylight savings")
                .withRecurRule("FREQ=DAILY;INTERVAL=1")
                .withTimezone("America/Chicago")
                .withStartTime("2020-05-28T01:00:00-05:00[America/Chicago]")
                .withEndTime(  "2020-05-28T02:00:00-05:00[America/Chicago]")
                .withRefTime(  "2021-11-16T01:00:00-06:00[America/Chicago]") // this is the second hour during the daylight transition
                                                                             // notice that the only thing that changed is the timezone,
                                                                             // but we're still at 1am
                .withExpected( "2021-11-17T01:00:00-06:00[America/Chicago]")
                .withExpectedActiveDurationMsec(TimeUnit.HOURS.toMillis(1))
                .check();

        // fall to spring counter part does not exist because 2am to 3am doesn't exist
        // since clocks go forward one hour
        // that 2am to 3am hour does not exist
    }

    /**
     * Test recurring daily perpetual schedule, every 2 days.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testNextOccurrenceRecurDailyTwoDaysPerpetual() throws Exception {
        testNextOccurrenceRecurDailyTwoDaysPerpetual(new ScheduleUtils(false));
        testNextOccurrenceRecurDailyTwoDaysPerpetual(new ScheduleUtils(true));
    }

    private void testNextOccurrenceRecurDailyTwoDaysPerpetual(ScheduleUtils scheduleUtils) throws Exception {
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The ref time is before the schedule begins, as a result the next"
                        + " occurrence should be the first occurrence of the schedule.")
                .withRecurRule("FREQ=DAILY;INTERVAL=2")
                .withStartTime("2010-01-10T09:30:00Z")
                .withRefTime(  "2010-01-10T09:00:00Z")
                .withExpected( "2010-01-10T09:30:00Z")
                .check();
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The refTime is already in the first occurrence for this schedule"
                        + " so we next occurrence should be in two days")
                .withRecurRule("FREQ=DAILY;INTERVAL=2")
                .withStartTime("2010-01-10T09:30:00Z")
                .withRefTime(  "2010-01-10T09:30:00Z")
                .withExpected( "2010-01-12T09:30:00Z")
                .check();
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The refTime is in the middle of the first occurrence for this schedule"
                        + " so the next occurrence should be in two days")
                .withRecurRule("FREQ=DAILY;INTERVAL=2")
                .withStartTime("2010-01-10T09:30:00Z")
                .withRefTime(  "2010-01-10T10:00:00Z")
                .withExpected( "2010-01-12T09:30:00Z")
                .check();
    }

    /**
     * Test recurring daily perpetual schedule, every 30 days.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testNextOccurrenceRecurDaily30DaysPerpetual() throws Exception {
        testNextOccurrenceRecurDaily30DaysPerpetual(new ScheduleUtils(false));
        testNextOccurrenceRecurDaily30DaysPerpetual(new ScheduleUtils(true));
    }

    private void testNextOccurrenceRecurDaily30DaysPerpetual(ScheduleUtils scheduleUtils) throws Exception {
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The ref time is before the schedule begins, as a result the next"
                        + " occurrence should be the first occurrence of the schedule.")
                .withRecurRule("FREQ=DAILY;INTERVAL=30")
                .withStartTime("2010-01-10T09:30:00Z")
                .withRefTime("2010-01-10T09:00:00Z")
                .withExpected("2010-01-10T09:30:00Z")
                .check();
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The refTime is already in the first occurrence for this schedule"
                        + " so we next occurrence should be in 30 days")
                .withRecurRule("FREQ=DAILY;INTERVAL=30")
                .withStartTime("2010-01-10T09:30:00Z")
                .withRefTime("2010-01-10T09:30:00Z")
                .withExpected("2010-02-09T09:30:00Z")
                .check();
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The refTime is in the middle of the first occurrence for this schedule"
                        + " so the next occurrence should be in 30 days")
                .withRecurRule("FREQ=DAILY;INTERVAL=30")
                .withStartTime("2010-01-10T09:30:00Z")
                .withRefTime("2010-01-10T10:00:00Z")
                .withExpected("2010-02-09T09:30:00Z")
                .check();
    }

    /**
     * Test recurring daily perpetual schedule with deferred recurrence start.
     *
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testNextOccurrenceRecurDailyPerpetualWithRecurStartTime() throws Exception {
        testNextOccurrenceRecurDailyPerpetualWithRecurStartTime(new ScheduleUtils(false));
        testNextOccurrenceRecurDailyPerpetualWithRecurStartTime(new ScheduleUtils(true));
    }

    private void testNextOccurrenceRecurDailyPerpetualWithRecurStartTime(ScheduleUtils scheduleUtils) throws Exception {
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setRecurrenceStart(RecurrenceStart.newBuilder()
                .setRecurrenceStartTime(RECURRENCE_START_TIME).build())
            .setPerpetual(Perpetual.getDefaultInstance())
            .setRecurRule("FREQ=DAILY;INTERVAL=2").build();
        // before schedule starts
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-18T09:30:00Z").toEpochMilli(),
                updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-18T09:30:00Z").toEpochMilli(),
                updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-01-19T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-20T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());
    }

    /**
     * Test recurring daily schedule with last date.
     *
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testNextOccurrenceRecurDailyWithLastDate() throws Exception {
        testNextOccurrenceRecurDailyWithLastDate(new ScheduleUtils(true));
        testNextOccurrenceRecurDailyWithLastDate(new ScheduleUtils(false));
    }

    private void testNextOccurrenceRecurDailyWithLastDate(ScheduleUtils scheduleUtils) throws Exception {
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setLastDate(LAST_DATE)
            .setRecurRule("FREQ=DAILY;INTERVAL=2").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(START_TIME, updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-12T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-01-10T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-12T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfterLastDate = Instant.parse("2011-01-01T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfterLastDate);
        assertFalse(updatedSchedule.hasNextOccurrence());
    }

    /**
     * Test recurring daily schedule with last date and deferred recurrence start time.
     *
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testNextOccurrenceRecurDailyWithLastDateAndRecurStartTime() throws Exception {
        testNextOccurrenceRecurDailyWithLastDateAndRecurStartTime(new ScheduleUtils(false));
        testNextOccurrenceRecurDailyWithLastDateAndRecurStartTime(new ScheduleUtils(true));
    }

    private void testNextOccurrenceRecurDailyWithLastDateAndRecurStartTime(ScheduleUtils scheduleUtils) throws Exception {
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setRecurrenceStart(RecurrenceStart.newBuilder()
                .setRecurrenceStartTime(RECURRENCE_START_TIME).build())
            .setLastDate(LAST_DATE)
            .setRecurRule("FREQ=DAILY;INTERVAL=2").build();
        // before schedule starts
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-18T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-18T09:30:00Z").toEpochMilli(),
                updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-01-10T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-18T09:30:00Z").toEpochMilli(),
                updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfterLastDate = Instant.parse("2011-01-01T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfterLastDate);
        assertFalse(updatedSchedule.hasNextOccurrence());
    }

    /**
     * Test recurring weekly perpetual schedule.
     *
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testNextOccurrenceRecurWeeklyPerpetual() throws Exception {
        testNextOccurrenceRecurWeeklyPerpetual(new ScheduleUtils(false));
        testNextOccurrenceRecurWeeklyPerpetual(new ScheduleUtils(true));
    }

    private void testNextOccurrenceRecurWeeklyPerpetual(ScheduleUtils scheduleUtils) throws Exception {
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setPerpetual(Perpetual.newBuilder().build())
            .setRecurRule("FREQ=WEEKLY;BYDAY=TU,WE;WKST=SU;INTERVAL=1").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-12T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-12T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-01-12T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-13T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());
    }

    /**
     * Test recurring weekly perpetual schedule with recurrence start time.
     *
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testNextOccurrenceRecurWeeklyPerpetualWithRecurStartTime() throws Exception {
        testNextOccurrenceRecurWeeklyPerpetualWithRecurStartTime(new ScheduleUtils(false));
        testNextOccurrenceRecurWeeklyPerpetualWithRecurStartTime(new ScheduleUtils(true));
    }

    private void testNextOccurrenceRecurWeeklyPerpetualWithRecurStartTime(ScheduleUtils scheduleUtils) throws Exception {
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setRecurrenceStart(RecurrenceStart.newBuilder()
                .setRecurrenceStartTime(RECURRENCE_START_TIME).build())
            .setPerpetual(Perpetual.getDefaultInstance())
            .setRecurRule("FREQ=WEEKLY;BYDAY=TU,WE;WKST=SU;INTERVAL=1").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-19T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-19T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-01-19T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-20T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());
    }

    /**
     * Test recurring weekly with last date.
     *
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testNextOccurrenceRecurWeeklyWithLast() throws Exception {
        testNextOccurrenceRecurWeeklyWithLast(new ScheduleUtils(false));
        testNextOccurrenceRecurWeeklyWithLast(new ScheduleUtils(true));
    }

    private void testNextOccurrenceRecurWeeklyWithLast(ScheduleUtils scheduleUtils) throws Exception {
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setLastDate(LAST_DATE)
            .setRecurRule("FREQ=WEEKLY;BYDAY=TU,WE;WKST=SU;INTERVAL=1").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-12T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-12T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-01-12T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-13T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfterLastDate = Instant.parse("2011-01-01T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfterLastDate);
        assertFalse(updatedSchedule.hasNextOccurrence());
    }

    /**
     * Test recurring weekly with last date.
     *
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testNextOccurrenceRecurWeeklyWithLastDateAndRecurrenceStartTime() throws Exception {
        testNextOccurrenceRecurWeeklyWithLastDateAndRecurrenceStartTime(new ScheduleUtils(false));
        testNextOccurrenceRecurWeeklyWithLastDateAndRecurrenceStartTime(new ScheduleUtils(true));
    }

    private void testNextOccurrenceRecurWeeklyWithLastDateAndRecurrenceStartTime(ScheduleUtils scheduleUtils) throws Exception {
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setRecurrenceStart(RecurrenceStart.newBuilder()
                .setRecurrenceStartTime(RECURRENCE_START_TIME).build())
            .setLastDate(LAST_DATE)
            .setRecurRule("FREQ=WEEKLY;BYDAY=TU,WE;WKST=SU;INTERVAL=1").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-19T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-19T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-01-19T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-20T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfterLastDate = Instant.parse("2011-01-01T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfterLastDate);
        assertFalse(updatedSchedule.hasNextOccurrence());
    }

    /**
     * Test bi-weekly recurring.
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testNextOccurrenceRecurBiWeekly() throws Exception {
        testNextOccurrenceRecurBiWeekly(new ScheduleUtils(false));
        testNextOccurrenceRecurBiWeekly(new ScheduleUtils(true));
    }

    private void testNextOccurrenceRecurBiWeekly(ScheduleUtils scheduleUtils) throws Exception {
        Schedule testScheduleBiWeekly = SCHEDULE.toBuilder()
            .setStartTime( Instant.parse("2010-01-15T09:30:00Z").toEpochMilli())
            .setLastDate(LAST_DATE)
            .setRecurRule("FREQ=WEEKLY;BYDAY=TU;INTERVAL=2;WKST=FR;")
            .build();

        final long periodStartBefore = Instant.parse("2010-01-15T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testScheduleBiWeekly.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-19T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testScheduleBiWeekly.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-19T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-01-20T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testScheduleBiWeekly.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-02-02T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfterLastDate = Instant.parse("2011-01-01T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testScheduleBiWeekly.toBuilder(), periodStartAfterLastDate);
        assertFalse(updatedSchedule.hasNextOccurrence());
    }

    /**
     * Test bi-weekly recurring.
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testNextOccurrenceRecurThreeWeekly() throws Exception {
        testNextOccurrenceRecurThreeWeekly(new ScheduleUtils(false));
        testNextOccurrenceRecurThreeWeekly(new ScheduleUtils(true));
    }

    private void testNextOccurrenceRecurThreeWeekly(ScheduleUtils scheduleUtils) throws Exception {
        Schedule testScheduleBiWeekly = SCHEDULE.toBuilder()
            .setStartTime( Instant.parse("2010-01-15T09:30:00Z").toEpochMilli())
            .setLastDate(LAST_DATE)
            .setRecurRule("FREQ=WEEKLY;BYDAY=WE;INTERVAL=3;WKST=FR;")
            .build();

        final long periodStartBefore = Instant.parse("2010-01-15T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testScheduleBiWeekly.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-20T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testScheduleBiWeekly.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-20T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-01-24T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testScheduleBiWeekly.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-02-10T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfterLastDate = Instant.parse("2011-01-01T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testScheduleBiWeekly.toBuilder(), periodStartAfterLastDate);
        assertFalse(updatedSchedule.hasNextOccurrence());
    }

    /**
     * Test recurring monthly by month day perpetual.
     *
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testNextOccurrenceRecurMonthlyByMonthDayPerpetual() throws Exception {
        testNextOccurrenceRecurMonthlyByMonthDayPerpetual(new ScheduleUtils(false));
        testNextOccurrenceRecurMonthlyByMonthDayPerpetual(new ScheduleUtils(true));
    }

    private void testNextOccurrenceRecurMonthlyByMonthDayPerpetual(ScheduleUtils scheduleUtils) throws Exception {
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The ref time is before the schedule starts, so the first occurrence"
                        + " of the schedule is the next occurrence.")
                .withRecurRule("FREQ=MONTHLY;BYMONTHDAY=15;INTERVAL=1")
                .withStartTime("2010-01-10T09:30:00Z")
                .withRefTime("2010-01-10T09:00:00Z")
                .withExpected("2010-01-15T09:30:00Z")
                .check();
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The ref time is after the schedule starts, but still before the first occurrence"
                        + " so the first occurrence is the next occurrence.")
                .withRecurRule("FREQ=MONTHLY;BYMONTHDAY=15;INTERVAL=1")
                .withStartTime("2010-01-10T09:30:00Z")
                .withRefTime("2010-01-10T09:30:00Z")
                .withExpected("2010-01-15T09:30:00Z")
                .check();
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The ref time is exactly the beginning of the first occurrence,"
                        + " so the second occurrence is the next occurence")
                .withRecurRule("FREQ=MONTHLY;BYMONTHDAY=15;INTERVAL=1")
                .withStartTime("2010-01-10T09:30:00Z")
                .withRefTime("2010-01-15T09:30:00Z")
                .withExpected("2010-02-15T09:30:00Z")
                .check();
        new RecurrenceTestCase()
                .withScheduleUtils(scheduleUtils)
                .withExplanation("The ref time is in the middle of the first occurrence,"
                        + " so the second occurrence is the next occurence")
                .withRecurRule("FREQ=MONTHLY;BYMONTHDAY=15;INTERVAL=1")
                .withStartTime("2010-01-10T09:30:00Z")
                .withRefTime("2010-01-15T10:00:00Z")
                .withExpected("2010-02-15T09:30:00Z")
                .check();
    }

    /**
     * Test recurring monthly by month day perpetual with deferred recurrence start time.
     *
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testNextOccurrenceRecurMonthlyByMonthDayPerpetualWithRecurStartTime() throws Exception {
        testNextOccurrenceRecurMonthlyByMonthDayPerpetualWithRecurStartTime(new ScheduleUtils(false));
        testNextOccurrenceRecurMonthlyByMonthDayPerpetualWithRecurStartTime(new ScheduleUtils(true));
    }

    private void testNextOccurrenceRecurMonthlyByMonthDayPerpetualWithRecurStartTime(ScheduleUtils scheduleUtils) throws Exception {
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setRecurrenceStart(RecurrenceStart.newBuilder()
                .setRecurrenceStartTime(RECURRENCE_START_TIME).build())
            .setPerpetual(Perpetual.getDefaultInstance())
            .setRecurRule("FREQ=MONTHLY;BYMONTHDAY=15;INTERVAL=1").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-02-15T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-02-15T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-02-15T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-03-15T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());
    }

    /**
     * Test recurring monthly by month day with last date.
     *
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testNextOccurrenceRecurMonthlyByMonthDayWithLastDate() throws Exception {
        testNextOccurrenceRecurMonthlyByMonthDayWithLastDate(new ScheduleUtils(false));
        testNextOccurrenceRecurMonthlyByMonthDayWithLastDate(new ScheduleUtils(true));
    }

    private void testNextOccurrenceRecurMonthlyByMonthDayWithLastDate(ScheduleUtils scheduleUtils) throws Exception {
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setLastDate(LAST_DATE)
            .setRecurRule("FREQ=MONTHLY;BYMONTHDAY=15;INTERVAL=1").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-15T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-15T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-01-15T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-02-15T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfterLastDate = Instant.parse("2011-01-01T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfterLastDate);
        assertFalse(updatedSchedule.hasNextOccurrence());
    }

    /**
     * Test recurring monthly by month day with last date and deferred recurrence start time.
     *
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testNextOccurrenceRecurMonthlyByMonthDayWithLastDateAndRecurStartTime() throws Exception {
        testNextOccurrenceRecurMonthlyByMonthDayWithLastDateAndRecurStartTime(new ScheduleUtils(false));
        testNextOccurrenceRecurMonthlyByMonthDayWithLastDateAndRecurStartTime(new ScheduleUtils(true));
    }

    private void testNextOccurrenceRecurMonthlyByMonthDayWithLastDateAndRecurStartTime(ScheduleUtils scheduleUtils) throws Exception {
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setRecurrenceStart(RecurrenceStart.newBuilder()
                .setRecurrenceStartTime(RECURRENCE_START_TIME).build())
            .setLastDate(LAST_DATE)
            .setRecurRule("FREQ=MONTHLY;BYMONTHDAY=15;INTERVAL=1").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-02-15T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-02-15T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-02-15T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-03-15T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfterLastDate = Instant.parse("2011-01-01T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfterLastDate);
        assertFalse(updatedSchedule.hasNextOccurrence());
    }

    /**
     * Test recurring monthly by set position perpetual.
     *
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testNextOccurrenceRecurMonthlySetPosPerpetual() throws Exception {
        testNextOccurrenceRecurMonthlySetPosPerpetual(new ScheduleUtils(false));
        testNextOccurrenceRecurMonthlySetPosPerpetual(new ScheduleUtils(true));
    }

    private void testNextOccurrenceRecurMonthlySetPosPerpetual(ScheduleUtils scheduleUtils) throws Exception {
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setPerpetual(Perpetual.getDefaultInstance())
            .setRecurRule("FREQ=MONTHLY;BYDAY=TU;BYSETPOS=4;INTERVAL=1").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-26T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-26T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-01-26T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-02-23T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());
    }

    /**
     * Test recurring monthly by set position perpetual with deferred recurrence start time.
     *
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testNextOccurrenceRecurMonthlySetPosPerpetualWithRecurStartTime() throws Exception {
        testNextOccurrenceRecurMonthlySetPosPerpetualWithRecurStartTime(new ScheduleUtils(false));
        testNextOccurrenceRecurMonthlySetPosPerpetualWithRecurStartTime(new ScheduleUtils(true));
    }

    private void testNextOccurrenceRecurMonthlySetPosPerpetualWithRecurStartTime(ScheduleUtils scheduleUtils) throws Exception {
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setRecurrenceStart(RecurrenceStart.newBuilder()
                .setRecurrenceStartTime(Instant.parse("2010-01-27T09:30:00Z").toEpochMilli()).build())
            .setPerpetual(Perpetual.getDefaultInstance())
            .setRecurRule("FREQ=MONTHLY;BYDAY=TU;BYSETPOS=4;INTERVAL=1").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-02-23T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-02-23T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-02-23T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-03-23T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());
    }

    /**
     * Test recurring monthly by set position with last date.
     *
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testNextOccurrenceRecurMonthlySetPosWithLastDate() throws Exception {
        testNextOccurrenceRecurMonthlySetPosWithLastDate(new ScheduleUtils(false));
        testNextOccurrenceRecurMonthlySetPosWithLastDate(new ScheduleUtils(true));
    }

    private void testNextOccurrenceRecurMonthlySetPosWithLastDate(ScheduleUtils scheduleUtils) throws Exception {
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setLastDate(LAST_DATE)
            .setRecurRule("FREQ=MONTHLY;BYDAY=TU;BYSETPOS=4;INTERVAL=1").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-26T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-26T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-01-26T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-02-23T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfterLastDate = Instant.parse("2011-01-01T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfterLastDate);
        assertFalse(updatedSchedule.hasNextOccurrence());
    }

    /**
     * Test recurring monthly by set position with last date.
     *
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testNextOccurrenceRecurMonthlySetPosWithLastDateAndRecurStartTime() throws Exception {
        testNextOccurrenceRecurMonthlySetPosWithLastDateAndRecurStartTime(new ScheduleUtils(false));
        testNextOccurrenceRecurMonthlySetPosWithLastDateAndRecurStartTime(new ScheduleUtils(true));
    }

    private void testNextOccurrenceRecurMonthlySetPosWithLastDateAndRecurStartTime(ScheduleUtils scheduleUtils) throws Exception {
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setRecurrenceStart(RecurrenceStart.newBuilder()
                .setRecurrenceStartTime(Instant.parse("2010-01-27T09:30:00Z").toEpochMilli()).build())
            .setLastDate(LAST_DATE)
            .setRecurRule("FREQ=MONTHLY;BYDAY=TU;BYSETPOS=4;INTERVAL=1").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-02-23T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-02-23T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-02-23T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-03-23T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfterLastDate = Instant.parse("2011-01-01T10:00:00Z").toEpochMilli();
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfterLastDate);
        assertFalse(updatedSchedule.hasNextOccurrence());
    }

    /**
     * Test calculate remaining time for one time schedule.
     *
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testRemainingTimeActiveOneTime() throws Exception {
        testRemainingTimeActiveOneTime(new ScheduleUtils(false));
        testRemainingTimeActiveOneTime(new ScheduleUtils(true));
    }

    private void testRemainingTimeActiveOneTime(ScheduleUtils scheduleUtils) throws Exception {
        final Schedule oneTimeSchedule = SCHEDULE.toBuilder().setOneTime(OneTime.newBuilder().build())
            .build();
        Schedule updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            oneTimeSchedule.toBuilder(), Instant.parse("2010-01-01T14:00:00Z").toEpochMilli());
        assertFalse(updatedSchedule.hasActive());
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            oneTimeSchedule.toBuilder(), Instant.parse("2010-01-10T09:30:00Z").toEpochMilli());
        assertTrue(updatedSchedule.hasActive());
        assertEquals(5.5 * 60 * 60 * 1000, updatedSchedule.getActive().getRemainingActiveTimeMs(), 0.01);
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            oneTimeSchedule.toBuilder(), Instant.parse("2010-01-10T13:30:00Z").toEpochMilli());
        assertTrue(updatedSchedule.hasActive());
        assertEquals(1.5 * 60 * 60 * 1000, updatedSchedule.getActive().getRemainingActiveTimeMs(), 0.01);
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            oneTimeSchedule.toBuilder(), Instant.parse("2010-01-10T14:00:00Z").toEpochMilli());
        assertTrue(updatedSchedule.hasActive());
        assertEquals(1 * 60 * 60 * 1000, updatedSchedule.getActive().getRemainingActiveTimeMs());
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            oneTimeSchedule.toBuilder(), Instant.parse("2010-01-10T15:00:00Z").toEpochMilli());
        assertFalse(updatedSchedule.hasActive());
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            oneTimeSchedule.toBuilder(), Instant.parse("2010-01-10T16:00:00Z").toEpochMilli());
        assertFalse(updatedSchedule.hasActive());
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            oneTimeSchedule.toBuilder(), Instant.parse("2010-01-11T09:30:00Z").toEpochMilli());
        assertFalse(updatedSchedule.hasActive());
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            oneTimeSchedule.toBuilder(), Instant.parse("2010-01-11T09:30:00Z").toEpochMilli());
        assertFalse(updatedSchedule.hasActive());
    }

    /**
     * Test calculate remaining time for recurring schedule.
     *
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testRemainingTimeActiveRecurring() throws Exception {
        testRemainingTimeActiveRecurring(new ScheduleUtils(false));
        testRemainingTimeActiveRecurring(new ScheduleUtils(true));
    }

    private void testRemainingTimeActiveRecurring(ScheduleUtils scheduleUtils) throws Exception {
        Schedule schedule = SCHEDULE.toBuilder()
            .setLastDate(LAST_DATE)
            .setRecurRule("FREQ=DAILY;INTERVAL=2").build();
        Schedule updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            schedule.toBuilder(), Instant.parse("2010-01-01T14:00:00Z").toEpochMilli());
        assertFalse(updatedSchedule.hasActive());
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            schedule.toBuilder(), Instant.parse("2010-01-10T09:30:00Z").toEpochMilli());
        assertTrue(updatedSchedule.hasActive());
        assertEquals(5.5 * 60 * 60 * 1000, updatedSchedule.getActive().getRemainingActiveTimeMs(), 0.01);
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            schedule.toBuilder(), Instant.parse("2010-01-10T13:30:00Z").toEpochMilli());
        assertTrue(updatedSchedule.hasActive());
        assertEquals(1.5 * 60 * 60 * 1000, updatedSchedule.getActive().getRemainingActiveTimeMs(), 0.01);
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            schedule.toBuilder(), Instant.parse("2010-01-10T14:00:00Z").toEpochMilli());
        assertTrue(updatedSchedule.hasActive());
        assertEquals(1 * 60 * 60 * 1000, updatedSchedule.getActive().getRemainingActiveTimeMs());
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            schedule.toBuilder(), Instant.parse("2010-01-10T15:00:00Z").toEpochMilli());
        assertFalse(updatedSchedule.hasActive());
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            schedule.toBuilder(), Instant.parse("2010-01-10T16:00:00Z").toEpochMilli());
        assertFalse(updatedSchedule.hasActive());
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            schedule.toBuilder(), Instant.parse("2010-01-11T16:00:00Z").toEpochMilli());
        assertFalse(updatedSchedule.hasActive());
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            schedule.toBuilder(), Instant.parse("2010-01-12T09:30:00Z").toEpochMilli());
        assertTrue(updatedSchedule.hasActive());
        assertEquals(5.5 * 60 * 60 * 1000, updatedSchedule.getActive().getRemainingActiveTimeMs(), 0.01);
        updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            schedule.toBuilder(), Instant.parse("2011-01-12T09:30:00Z").toEpochMilli());
        assertFalse(updatedSchedule.hasActive());
    }

    /**
     * Test for {@link ParseException}.
     *
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testInvalidRecurrenceRuleException() throws Exception {
        final Schedule schedule = Schedule.newBuilder()
                .setPerpetual(Perpetual.newBuilder().build())
                .setTimezoneId(ZoneId.systemDefault().getId())
                .setRecurRule("FREQ=DAILY;INTERVAL=2;UNTIL=INVALID").build();
        thrown.expect(InvalidRecurrenceRuleException.class);
        new ScheduleUtils(true).calculateNextOccurrenceAndRemainingTimeActive(schedule.toBuilder(),
                Instant.parse("2010-01-10T15:00:00Z").toEpochMilli());
    }

    /**
     * Test for {@link ParseException}.
     *
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testParseException() throws Exception {
        final Schedule schedule = Schedule.newBuilder()
                .setPerpetual(Perpetual.newBuilder().build())
                .setTimezoneId(ZoneId.systemDefault().getId())
                .setRecurRule("FREQ=DAILY;INTERVAL=2;UNTIL=INVALID").build();
        thrown.expect(ParseException.class);
        new ScheduleUtils(false).calculateNextOccurrenceAndRemainingTimeActive(schedule.toBuilder(),
                Instant.parse("2010-01-10T15:00:00Z").toEpochMilli());
    }

    private static String generateExplanation(
            long expected,
            long actual,
            String timezone
    ) {
        final String actualStr = ZonedDateTime.ofInstant(Instant.ofEpochMilli(actual), ZoneId.of(timezone)).toString();
        final String expectedStr = ZonedDateTime.ofInstant(Instant.ofEpochMilli(expected), ZoneId.of(timezone)).toString();
        return "expected epoch=" + expected
                + "\nactual epoch  =" + actual
                + "\nexpected =" + expectedStr
                + "\nactual   =" + actualStr;
    }

    /**
     * A builder class to make constructing recurrence test cases easier.
     */
    private static class RecurrenceTestCase {

        private ScheduleUtils scheduleUtils;
        private String explanation;
        private String startTime;
        private String endTime;
        private String recurRule;
        private String refTime;
        private String expected;
        private String timezone;
        private Long expectedActiveDurationMsec;

        public RecurrenceTestCase withScheduleUtils(ScheduleUtils scheduleUtils) {
            this.scheduleUtils = scheduleUtils;
            return this;
        }

        public RecurrenceTestCase withExplanation(String explanation) {
            this.explanation = explanation;
            return this;
        }

        public RecurrenceTestCase withTimezone(String timezone) {
            this.timezone = timezone;
            return this;
        }

        public RecurrenceTestCase withStartTime(String startTime) {
            this.startTime = startTime;
            return this;
        }

        public RecurrenceTestCase withEndTime(String endTime) {
            this.endTime = endTime;
            return this;
        }

        public RecurrenceTestCase withRecurRule(String recurRule) {
            this.recurRule = recurRule;
            return this;
        }

        public RecurrenceTestCase withRefTime(String refTime) {
            this.refTime = refTime;
            return this;
        }

        public RecurrenceTestCase withExpected(String expected) {
            this.expected = expected;
            return this;
        }

        public RecurrenceTestCase withExpectedActiveDurationMsec(long expectedActiveDurationMsec) {
            this.expectedActiveDurationMsec = expectedActiveDurationMsec;
            return this;
        }

        public void check() throws Exception {
            Objects.requireNonNull(scheduleUtils);
            Objects.requireNonNull(explanation);
            Objects.requireNonNull(startTime);
            Objects.requireNonNull(recurRule);
            Objects.requireNonNull(refTime);
            Objects.requireNonNull(expected);

            Schedule.Builder testSchedulePerpetualBuilder = SCHEDULE.toBuilder()
                    .setStartTime(DateTimeFormatter.ISO_DATE_TIME.parse(startTime, Instant::from).toEpochMilli())
                    .setPerpetual(Perpetual.getDefaultInstance())
                    .setRecurRule(recurRule);
            if (endTime != null) {
                testSchedulePerpetualBuilder.setEndTime(DateTimeFormatter.ISO_DATE_TIME.parse(endTime, Instant::from).toEpochMilli());
            }
            if (timezone != null) {
                testSchedulePerpetualBuilder.setTimezoneId(ZoneId.of(timezone).getId());
            }
            Schedule testSchedulePerpetual = testSchedulePerpetualBuilder.build();

            // before schedule starts
            final long periodStartBefore = DateTimeFormatter.ISO_DATE_TIME.parse(refTime, Instant::from).toEpochMilli();
            Schedule updatedSchedule = scheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
                    testSchedulePerpetual.toBuilder(), periodStartBefore);
            assertTrue(explanation, updatedSchedule.hasNextOccurrence());
            final long actualEpoch = updatedSchedule.getNextOccurrence().getStartTime();
            final long expectedEpoch = DateTimeFormatter.ISO_DATE_TIME.parse(expected, Instant::from).toEpochMilli();
            assertEquals(explanation
                            + "\n" + generateExplanation(expectedEpoch, actualEpoch, updatedSchedule.getTimezoneId()),
                    expectedEpoch,
                    actualEpoch);
            if (expectedActiveDurationMsec != null) {
                assertTrue(updatedSchedule.hasActive());
                assertTrue(updatedSchedule.getActive().hasRemainingActiveTimeMs());
                assertEquals(expectedActiveDurationMsec.longValue(), updatedSchedule.getActive().getRemainingActiveTimeMs());
            }
        }

    }
}
