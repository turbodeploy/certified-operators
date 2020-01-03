package com.vmturbo.group.schedule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneId;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.OneTime;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.Perpetual;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.RecurrenceStart;

/**
 * Unit tests for {@link ScheduleUtils}.
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
        final Schedule oneTimeSchedule = SCHEDULE.toBuilder().setOneTime(OneTime.newBuilder().build())
            .build();
        // before schedule starts
        Schedule schedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            oneTimeSchedule.toBuilder(), Instant.parse("2010-01-01T09:30:00Z").toEpochMilli());
        assertTrue(schedule.hasNextOccurrence());
        assertEquals(START_TIME, schedule.getNextOccurrence().getStartTime());

        schedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            oneTimeSchedule.toBuilder(), SCHEDULE.getStartTime());
        assertFalse(schedule.hasNextOccurrence());

        schedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            oneTimeSchedule.toBuilder(), Instant.parse("2012-01-01T09:30:00Z").toEpochMilli());
        assertFalse(schedule.hasNextOccurrence());
    }

    /**
     * Test recurring daily perpetual schedule.
     *
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testNextOccurrenceRecurDailyPerpetual() throws Exception {
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setPerpetual(Perpetual.getDefaultInstance())
            .setRecurRule("FREQ=DAILY;INTERVAL=2").build();
        // before schedule starts
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(START_TIME, updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-12T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-01-10T10:00:00Z").toEpochMilli();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-12T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());
    }

    /**
     * Test recurring daily perpetual schedule with deferred recurrence start.
     *
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testNextOccurrenceRecurDailyPerpetualWithRecurStartTime() throws Exception {
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setRecurrenceStart(RecurrenceStart.newBuilder()
                .setRecurrenceStartTime(RECURRENCE_START_TIME).build())
            .setPerpetual(Perpetual.getDefaultInstance())
            .setRecurRule("FREQ=DAILY;INTERVAL=2").build();
        // before schedule starts
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-18T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-18T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-01-19T10:00:00Z").toEpochMilli();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
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
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setLastDate(LAST_DATE)
            .setRecurRule("FREQ=DAILY;INTERVAL=2").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(START_TIME, updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-12T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-01-10T10:00:00Z").toEpochMilli();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-12T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfterLastDate = Instant.parse("2011-01-01T10:00:00Z").toEpochMilli();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
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
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setRecurrenceStart(RecurrenceStart.newBuilder()
                .setRecurrenceStartTime(RECURRENCE_START_TIME).build())
            .setLastDate(LAST_DATE)
            .setRecurRule("FREQ=DAILY;INTERVAL=2").build();
        // before schedule starts
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-18T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-18T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-01-10T10:00:00Z").toEpochMilli();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-18T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfterLastDate = Instant.parse("2011-01-01T10:00:00Z").toEpochMilli();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
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
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setPerpetual(Perpetual.newBuilder().build())
            .setRecurRule("FREQ=WEEKLY;BYDAY=TU,WE;INTERVAL=1").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-12T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-12T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-01-12T10:00:00Z").toEpochMilli();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
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
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setRecurrenceStart(RecurrenceStart.newBuilder()
                .setRecurrenceStartTime(RECURRENCE_START_TIME).build())
            .setPerpetual(Perpetual.getDefaultInstance())
            .setRecurRule("FREQ=WEEKLY;BYDAY=TU,WE;INTERVAL=1").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-19T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-19T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-01-19T10:00:00Z").toEpochMilli();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
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
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setLastDate(LAST_DATE)
            .setRecurRule("FREQ=WEEKLY;BYDAY=TU,WE;INTERVAL=1").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-12T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-12T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-01-12T10:00:00Z").toEpochMilli();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-13T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfterLastDate = Instant.parse("2011-01-01T10:00:00Z").toEpochMilli();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
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
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setRecurrenceStart(RecurrenceStart.newBuilder()
                .setRecurrenceStartTime(RECURRENCE_START_TIME).build())
            .setLastDate(LAST_DATE)
            .setRecurRule("FREQ=WEEKLY;BYDAY=TU,WE;INTERVAL=1").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-19T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-19T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-01-19T10:00:00Z").toEpochMilli();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-20T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfterLastDate = Instant.parse("2011-01-01T10:00:00Z").toEpochMilli();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfterLastDate);
        assertFalse(updatedSchedule.hasNextOccurrence());
    }

    /**
     * Test recurring monthly by month day perpetual.
     *
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testNextOccurrenceRecurMonthlyByMonthDayPerpetual() throws Exception {
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setPerpetual(Perpetual.getDefaultInstance())
            .setRecurRule("FREQ=MONTHLY;BYMONTHDAY=15;INTERVAL=1").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-15T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-15T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-01-15T10:00:00Z").toEpochMilli();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-02-15T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());
    }

    /**
     * Test recurring monthly by month day perpetual with deferred recurrence start time.
     *
     * @throws Exception If any unexpected exceptions
     */
    @Test
    public void testNextOccurrenceRecurMonthlyByMonthDayPerpetualWithRecurStartTime() throws Exception {
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setRecurrenceStart(RecurrenceStart.newBuilder()
                .setRecurrenceStartTime(RECURRENCE_START_TIME).build())
            .setPerpetual(Perpetual.getDefaultInstance())
            .setRecurRule("FREQ=MONTHLY;BYMONTHDAY=15;INTERVAL=1").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-02-15T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-02-15T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-02-15T10:00:00Z").toEpochMilli();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
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
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setLastDate(LAST_DATE)
            .setRecurRule("FREQ=MONTHLY;BYMONTHDAY=15;INTERVAL=1").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-15T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-15T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-01-15T10:00:00Z").toEpochMilli();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-02-15T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfterLastDate = Instant.parse("2011-01-01T10:00:00Z").toEpochMilli();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
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
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setRecurrenceStart(RecurrenceStart.newBuilder()
                .setRecurrenceStartTime(RECURRENCE_START_TIME).build())
            .setLastDate(LAST_DATE)
            .setRecurRule("FREQ=MONTHLY;BYMONTHDAY=15;INTERVAL=1").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-02-15T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-02-15T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-02-15T10:00:00Z").toEpochMilli();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-03-15T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfterLastDate = Instant.parse("2011-01-01T10:00:00Z").toEpochMilli();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
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
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setPerpetual(Perpetual.getDefaultInstance())
            .setRecurRule("FREQ=MONTHLY;BYDAY=TU;BYSETPOS=4;INTERVAL=1").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-26T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-26T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-01-26T10:00:00Z").toEpochMilli();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
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
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setRecurrenceStart(RecurrenceStart.newBuilder()
                .setRecurrenceStartTime(Instant.parse("2010-01-27T09:30:00Z").toEpochMilli()).build())
            .setPerpetual(Perpetual.getDefaultInstance())
            .setRecurRule("FREQ=MONTHLY;BYDAY=TU;BYSETPOS=4;INTERVAL=1").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-02-23T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-02-23T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-02-23T10:00:00Z").toEpochMilli();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
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
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setLastDate(LAST_DATE)
            .setRecurRule("FREQ=MONTHLY;BYDAY=TU;BYSETPOS=4;INTERVAL=1").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-26T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-01-26T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-01-26T10:00:00Z").toEpochMilli();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-02-23T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfterLastDate = Instant.parse("2011-01-01T10:00:00Z").toEpochMilli();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
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
        Schedule testSchedulePerpetual = SCHEDULE.toBuilder()
            .setRecurrenceStart(RecurrenceStart.newBuilder()
                .setRecurrenceStartTime(Instant.parse("2010-01-27T09:30:00Z").toEpochMilli()).build())
            .setLastDate(LAST_DATE)
            .setRecurRule("FREQ=MONTHLY;BYDAY=TU;BYSETPOS=4;INTERVAL=1").build();
        final long periodStartBefore = Instant.parse("2010-01-10T09:00:00Z").toEpochMilli();
        Schedule updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartBefore);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-02-23T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStart = SCHEDULE.getStartTime();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStart);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-02-23T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfter = Instant.parse("2010-02-23T10:00:00Z").toEpochMilli();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            testSchedulePerpetual.toBuilder(), periodStartAfter);
        assertTrue(updatedSchedule.hasNextOccurrence());
        assertEquals(Instant.parse("2010-03-23T09:30:00Z").toEpochMilli(),
            updatedSchedule.getNextOccurrence().getStartTime());

        final long periodStartAfterLastDate = Instant.parse("2011-01-01T10:00:00Z").toEpochMilli();
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
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
        final Schedule oneTimeSchedule = SCHEDULE.toBuilder().setOneTime(OneTime.newBuilder().build())
            .build();
        Schedule updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            oneTimeSchedule.toBuilder(), Instant.parse("2010-01-01T14:00:00Z").toEpochMilli());
        assertFalse(updatedSchedule.hasActive());
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            oneTimeSchedule.toBuilder(), Instant.parse("2010-01-10T09:30:00Z").toEpochMilli());
        assertTrue(updatedSchedule.hasActive());
        assertEquals(5.5 * 60 * 60 * 1000, updatedSchedule.getActive().getRemainingActiveTimeMs(), 0.01);
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            oneTimeSchedule.toBuilder(), Instant.parse("2010-01-10T13:30:00Z").toEpochMilli());
        assertTrue(updatedSchedule.hasActive());
        assertEquals(1.5 * 60 * 60 * 1000, updatedSchedule.getActive().getRemainingActiveTimeMs(), 0.01);
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            oneTimeSchedule.toBuilder(), Instant.parse("2010-01-10T14:00:00Z").toEpochMilli());
        assertTrue(updatedSchedule.hasActive());
        assertEquals(1 * 60 * 60 * 1000, updatedSchedule.getActive().getRemainingActiveTimeMs());
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            oneTimeSchedule.toBuilder(), Instant.parse("2010-01-10T15:00:00Z").toEpochMilli());
        assertFalse(updatedSchedule.hasActive());
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            oneTimeSchedule.toBuilder(), Instant.parse("2010-01-10T16:00:00Z").toEpochMilli());
        assertFalse(updatedSchedule.hasActive());
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            oneTimeSchedule.toBuilder(), Instant.parse("2010-01-11T09:30:00Z").toEpochMilli());
        assertFalse(updatedSchedule.hasActive());
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
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
        Schedule schedule = SCHEDULE.toBuilder()
            .setLastDate(LAST_DATE)
            .setRecurRule("FREQ=DAILY;INTERVAL=2").build();
        Schedule updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            schedule.toBuilder(), Instant.parse("2010-01-01T14:00:00Z").toEpochMilli());
        assertFalse(updatedSchedule.hasActive());
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            schedule.toBuilder(), Instant.parse("2010-01-10T09:30:00Z").toEpochMilli());
        assertTrue(updatedSchedule.hasActive());
        assertEquals(5.5 * 60 * 60 * 1000, updatedSchedule.getActive().getRemainingActiveTimeMs(), 0.01);
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            schedule.toBuilder(), Instant.parse("2010-01-10T13:30:00Z").toEpochMilli());
        assertTrue(updatedSchedule.hasActive());
        assertEquals(1.5 * 60 * 60 * 1000, updatedSchedule.getActive().getRemainingActiveTimeMs(), 0.01);
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            schedule.toBuilder(), Instant.parse("2010-01-10T14:00:00Z").toEpochMilli());
        assertTrue(updatedSchedule.hasActive());
        assertEquals(1 * 60 * 60 * 1000, updatedSchedule.getActive().getRemainingActiveTimeMs());
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            schedule.toBuilder(), Instant.parse("2010-01-10T15:00:00Z").toEpochMilli());
        assertFalse(updatedSchedule.hasActive());
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            schedule.toBuilder(), Instant.parse("2010-01-10T16:00:00Z").toEpochMilli());
        assertFalse(updatedSchedule.hasActive());
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            schedule.toBuilder(), Instant.parse("2010-01-11T16:00:00Z").toEpochMilli());
        assertFalse(updatedSchedule.hasActive());
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            schedule.toBuilder(), Instant.parse("2010-01-12T09:30:00Z").toEpochMilli());
        assertTrue(updatedSchedule.hasActive());
        assertEquals(5.5 * 60 * 60 * 1000, updatedSchedule.getActive().getRemainingActiveTimeMs(), 0.01);
        updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
            schedule.toBuilder(), Instant.parse("2011-01-12T09:30:00Z").toEpochMilli());
        assertFalse(updatedSchedule.hasActive());
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
        ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(schedule.toBuilder(),
            Instant.parse("2010-01-10T15:00:00Z").toEpochMilli());
    }

}
