package com.vmturbo.topology.processor.group.settings;

import static java.time.temporal.ChronoUnit.MINUTES;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.vmturbo.common.protobuf.setting.SettingProto.Schedule;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule.Daily;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule.DayOfWeek;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule.Monthly;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule.OneTime;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule.Perpetual;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule.Weekly;

public class ScheduleResolverTest {

    private final LocalTime beforeTime = LocalTime.of(1, 15, 40);
    private final LocalTime startTime = LocalTime.of(6, 12, 11);
    private final LocalTime duringTime = LocalTime.of(8, 55, 20);
    private final LocalTime endTime = LocalTime.of(10, 25, 11);
    private final LocalTime afterTime = LocalTime.of(20, 30, 11);
    private final LocalTime testEndTime = LocalTime.of(3, 10, 10);
    private final LocalTime zeroTime = LocalTime.of(0, 0, 0);

    private final LocalDate beforeDate = LocalDate.of(2005, 9, 27); //A Fever You Can't Sweat Out
    private final LocalDate startDate = LocalDate.of(2008, 3, 21); //Pretty. Odd.
    private final LocalDate duringDate = LocalDate.of(2011, 3, 22); //Vices and Virtues
    private final LocalDate endDate = LocalDate.of(2013, 10, 8); //Too Weird To Live, Too Rare To Die
    private final LocalDate afterDate = LocalDate.of(2016, 1, 15); //Death of a Bachelor

    private final Instant beforeActiveDaysBegin = makeInstant(beforeDate, duringTime);
    private final Instant startOfFirstActivePeriod = makeInstant(startDate, startTime);
    private final Instant endOfFirstActivePeriod = makeInstant(startDate, endTime);
    private final Instant endOfFirsActiveWithDifferentDay = makeInstant(startDate.plusDays(1), testEndTime);
    private final Instant endOfFirsActiveLastDay = makeInstant(startDate.plusDays(1), zeroTime);

    private final Instant duringFirstActivePeriod = makeInstant(startDate, duringTime);
    private final Instant afterLastActivePeriod =  makeInstant(endDate, afterTime);
    private final Instant afterActiveDate = makeInstant(afterDate, duringTime);

    @Test
    public void testOneTimeWithEndTime() {

        final Schedule schedule = Schedule.newBuilder()
                .setOneTime(OneTime.getDefaultInstance())
                .setStartTime(startOfFirstActivePeriod.toEpochMilli())
                .setEndTime(endOfFirstActivePeriod.toEpochMilli())
                .build();


        assertFalse(scheduleAppliesAt(beforeActiveDaysBegin, schedule));
        testActivePeriod(startDate, schedule);
        assertFalse(scheduleAppliesAt(makeInstant(duringDate, duringTime), schedule));
    }

    @Test
    public void testOneTimeWithMinutes() {

        final Schedule schedule = Schedule.newBuilder()
                .setOneTime(OneTime.getDefaultInstance())
                .setStartTime(startOfFirstActivePeriod.toEpochMilli())
                //max of 1440 mins in day so this is safe:
                .setMinutes((int) MINUTES.between(startTime, endTime))
                .build();


        assertFalse(scheduleAppliesAt(beforeActiveDaysBegin, schedule));
        testActivePeriod(startDate, schedule);
        assertFalse(scheduleAppliesAt(makeInstant(duringDate, duringTime), schedule));
    }

    @Test
    public void testOneTimeWithCrossDayCase() {
        final Schedule schedule = Schedule.newBuilder()
                .setOneTime(OneTime.getDefaultInstance())
                .setStartTime(startOfFirstActivePeriod.toEpochMilli())
                .setEndTime(endOfFirsActiveWithDifferentDay.toEpochMilli())
                .setLastDate(endOfFirsActiveLastDay.toEpochMilli())
                .build();
        assertFalse(scheduleAppliesAt(beforeActiveDaysBegin, schedule));
        testActivePeriodWithCrossDay(startDate, schedule);
    }

    @Test
    public void testDailyWithEndDay() {


        final Schedule schedule = Schedule.newBuilder()
                .setDaily(Daily.getDefaultInstance())
                .setStartTime(startOfFirstActivePeriod.toEpochMilli())
                .setEndTime(endOfFirstActivePeriod.toEpochMilli())
                .setLastDate(afterLastActivePeriod.toEpochMilli())
                .build();

        assertFalse(scheduleAppliesAt(beforeActiveDaysBegin, schedule));
        testActivePeriod(startDate, schedule);
        testActivePeriod(endDate, schedule);
        assertFalse(scheduleAppliesAt(afterActiveDate, schedule));

    }

    @Test
    public void testDailyPerpetual() {

        final Schedule schedule = Schedule.newBuilder()
                .setDaily(Daily.getDefaultInstance())
                .setStartTime(startOfFirstActivePeriod.toEpochMilli())
                .setEndTime(endOfFirstActivePeriod.toEpochMilli())
                .setPerpetual(Perpetual.getDefaultInstance())
                .build();

        assertFalse(scheduleAppliesAt(beforeActiveDaysBegin, schedule));
        testActivePeriod(startDate, schedule);

        testActivePeriod(LocalDate.of(3018, 1, 2), schedule);
    }

    @Test
    public void testDailyWithCrossDayCase() {
        final Schedule schedule = Schedule.newBuilder()
                .setDaily(Daily.getDefaultInstance())
                .setStartTime(startOfFirstActivePeriod.toEpochMilli())
                .setEndTime(endOfFirsActiveWithDifferentDay.toEpochMilli())
                .setLastDate(endOfFirsActiveLastDay.toEpochMilli())
                .build();
        assertFalse(scheduleAppliesAt(beforeActiveDaysBegin, schedule));
        testActivePeriodWithCrossDay(startDate, schedule);
    }

    @Test
    public void testWeeklyWithDaysNotOnStartOrEnd() {

        final List<DayOfWeek> activeDays = Arrays.asList(DayOfWeek.MONDAY, DayOfWeek.SATURDAY);
        final LocalDate firstActualDay = startDate.plusDays(1);
        final LocalDate secondActualDay = firstActualDay.plusDays(2);

        assertFalse(activeDays.contains(DayOfWeek.valueOf(startDate.getDayOfWeek().name())));
        assertFalse(activeDays.contains(DayOfWeek.valueOf(endDate.getDayOfWeek().name())));
        assertTrue(activeDays.contains(DayOfWeek.valueOf(firstActualDay.getDayOfWeek().name())));
        assertTrue(activeDays.contains(DayOfWeek.valueOf(secondActualDay.getDayOfWeek().name())));

        //note: as of the creation of this test, start date is Friday and end date is Tuesday.
        //further change to the test start date and end date may require different active days
        //if the above asserts fail, it's just because the active days and the start/end dates are
        //out of sync in this test.


        final Schedule schedule = Schedule.newBuilder()
                .setWeekly(Weekly.newBuilder().addAllDaysOfWeek(activeDays))
                .setStartTime(startOfFirstActivePeriod.toEpochMilli())
                .setEndTime(endOfFirstActivePeriod.toEpochMilli())
                .setLastDate(afterLastActivePeriod.toEpochMilli())
                .build();


        assertFalse(scheduleAppliesAt(beforeActiveDaysBegin, schedule));
        assertFalse(scheduleAppliesAt(duringFirstActivePeriod, schedule));

        testActivePeriod(firstActualDay, schedule);
        assertFalse(scheduleAppliesAt(makeInstant(firstActualDay.plusDays(1), duringTime), schedule));
        testActivePeriod(secondActualDay, schedule);
        testActivePeriod(firstActualDay.plusWeeks(1), schedule);
        testActivePeriod(secondActualDay.plusWeeks(1), schedule);
        assertFalse(scheduleAppliesAt(makeInstant(endDate, duringTime), schedule));
        assertFalse(scheduleAppliesAt(makeInstant(endDate.plusDays(4), duringTime), schedule));
        assertFalse(scheduleAppliesAt(makeInstant(endDate.plusDays(6), duringTime), schedule));

    }

    @Test
    public void testWeeklyWithStartDay() {
        final Schedule schedule = Schedule.newBuilder()
                .setWeekly(Weekly.newBuilder().addDaysOfWeek(DayOfWeek.valueOf(startDate.getDayOfWeek().name())))
                .setStartTime(startOfFirstActivePeriod.toEpochMilli())
                .setEndTime(endOfFirstActivePeriod.toEpochMilli())
                .setLastDate(afterLastActivePeriod.toEpochMilli())
                .build();

        testActivePeriod(startDate, schedule);
        testActivePeriod(startDate.plusWeeks(1), schedule);
        assertFalse(scheduleAppliesAt(makeInstant(startDate.plusDays(3), duringTime), schedule));
        assertFalse(scheduleAppliesAt(makeInstant(endDate, duringTime), schedule));
    }

    @Test
    public void testMonthlyWithDaysNotOnStartOrEnd() {

        final List<Integer> activeDays = Arrays.asList(2, 6);

        assertFalse(activeDays.contains(startDate.getDayOfMonth()));
        assertFalse(activeDays.contains(endDate.getDayOfMonth()));

        //note: as of the creation of this test, start date is the 21st and end date is the 8th.
        //further change to the test start date and end date may require different active days
        //if the above asserts fail, it's just because the active days and the start/end dates are
        //out of sync in this test.

        final Schedule schedule = Schedule.newBuilder()
                .setMonthly(Monthly.newBuilder().addAllDaysOfMonth(activeDays).build())
                .setStartTime(startOfFirstActivePeriod.toEpochMilli())
                .setEndTime(endOfFirstActivePeriod.toEpochMilli())
                .setLastDate(afterLastActivePeriod.toEpochMilli())
                .build();

        assertFalse(scheduleAppliesAt(makeInstant(startDate, duringTime), schedule));
        assertFalse(scheduleAppliesAt(makeInstant(startDate.withDayOfMonth(2), duringTime), schedule));
        assertFalse(scheduleAppliesAt(makeInstant(startDate.withDayOfMonth(6), duringTime), schedule));
        testActivePeriod(startDate.plusMonths(1).withDayOfMonth(2), schedule);
        testActivePeriod(startDate.plusMonths(1).withDayOfMonth(6), schedule);
        assertFalse(scheduleAppliesAt(makeInstant(startDate.plusMonths(1), duringTime), schedule));
        testActivePeriod(endDate.withDayOfMonth(2), schedule);
        testActivePeriod(endDate.withDayOfMonth(6), schedule);
        assertFalse(scheduleAppliesAt(makeInstant(endDate, duringTime), schedule));

    }

    @Test
    public void testMonthlyWithStartDay() {
        final Schedule schedule = Schedule.newBuilder()
                .setMonthly(Monthly.newBuilder().addDaysOfMonth(startDate.getDayOfMonth()).build())
                .setStartTime(startOfFirstActivePeriod.toEpochMilli())
                .setEndTime(endOfFirstActivePeriod.toEpochMilli())
                .setLastDate(afterLastActivePeriod.toEpochMilli())
                .build();

        assertFalse(scheduleAppliesAt(beforeActiveDaysBegin, schedule));
        testActivePeriod(startDate, schedule);
        assertFalse(scheduleAppliesAt(makeInstant(startDate.plusDays(4), duringTime), schedule));
        testActivePeriod(startDate.plusMonths(2), schedule);
        assertFalse(scheduleAppliesAt(makeInstant(endDate, duringTime), schedule));
    }


    private void testActivePeriod(LocalDate date, Schedule schedule) {
        assertFalse(scheduleAppliesAt(makeInstant(date, beforeTime), schedule));
        assertTrue(scheduleAppliesAt(makeInstant(date, startTime), schedule));
        assertTrue(scheduleAppliesAt(makeInstant(date, duringTime), schedule));
        assertTrue(scheduleAppliesAt(makeInstant(date, endTime), schedule));
        assertFalse(scheduleAppliesAt(makeInstant(date, afterTime), schedule));
    }

    private void testActivePeriodWithCrossDay(LocalDate date, Schedule schedule) {
        assertFalse(scheduleAppliesAt(makeInstant(date, beforeTime), schedule));
        assertTrue(scheduleAppliesAt(makeInstant(date, startTime), schedule));
        assertTrue(scheduleAppliesAt(makeInstant(date, duringTime), schedule));
        assertTrue(scheduleAppliesAt(makeInstant(date, endTime), schedule));
        assertTrue(scheduleAppliesAt(makeInstant(date, afterTime), schedule));
        assertTrue(scheduleAppliesAt(makeInstant(date.plusDays(1), beforeTime), schedule));
        assertTrue(scheduleAppliesAt(makeInstant(date.plusDays(1), testEndTime), schedule));
        assertFalse(scheduleAppliesAt(makeInstant(date.plusDays(1), endTime), schedule));
    }

    private boolean scheduleAppliesAt(Instant instant, Schedule schedule) {
        return new ScheduleResolver(instant).appliesAtResolutionInstant(schedule);
    }

    private Instant makeInstant(final LocalDate date, final LocalTime time) {
        return LocalDateTime.of(date, time).toInstant(ZoneOffset.UTC);
    }


}
