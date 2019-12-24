package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.api.dto.settingspolicy.RecurrenceApiDTO;
import com.vmturbo.api.dto.settingspolicy.ScheduleApiDTO;
import com.vmturbo.api.enums.DayOfWeek;
import com.vmturbo.api.enums.RecurrenceType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.Active;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.NextOccurrence;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.OneTime;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.RecurrenceStart;

/**
 * Unit tests for {@link ScheduleMapper}.
 */
public class ScheduleMapperTest {
    private static final String ID = "1";
    private static final String DISPLAY_NAME = "Test Display Name";
    private static final String TIMEZONE = "America/New_York";
    private static final LocalDateTime START_TIME =
        LocalDateTime.of(2015, 12, 10, 12, 30);
    private static final long START_TIME_TS =
        START_TIME.atZone(ZoneId.of(TIMEZONE)).toInstant().toEpochMilli();
    private static final LocalDateTime END_TIME =
        LocalDateTime.of(2015, 12, 10, 13, 30);
    private static final long END_TIME_TS =
        END_TIME.atZone(ZoneId.of(TIMEZONE)).toInstant().toEpochMilli();
    private static final LocalDateTime START_DATE =
        LocalDateTime.of(2015, 12, 11, 13, 30);
    private static final long START_DATE_TS =
        START_DATE.atZone(ZoneId.of(TIMEZONE)).toInstant().toEpochMilli();
    private static final LocalDate END_DATE = LocalDate.of(2016, 12, 31);
    private static final long END_DATE_TS =
        LocalDateTime.of(END_DATE, LocalTime.MAX).atZone(ZoneId.of(TIMEZONE)).toInstant().toEpochMilli();

    private static final String NEXT_OCCURRENCE = "2015-12-12T12:30:00";
    private static final long NEXT_OCCURRENCE_TIMESTAMP = 1449941400000L;
    private static final long REMAINING_TIME_ACTIVE = 157602148200000L;

    /** Expected exceptions to test against. */
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private final ScheduleMapper scheduleMapper = new ScheduleMapper();


    @Nonnull
    private ScheduleApiDTO createScheduleApiDto() {
        ScheduleApiDTO scheduleApiDTO = new ScheduleApiDTO();
        scheduleApiDTO.setDisplayName(DISPLAY_NAME);
        scheduleApiDTO.setStartTime(START_TIME);
        scheduleApiDTO.setEndTime(END_TIME);
        scheduleApiDTO.setTimeZone(TIMEZONE);

        return scheduleApiDTO;
    }

    private RecurrenceApiDTO createRecurrenceApiDto(@Nonnull RecurrenceType type,
            @Nullable Integer interval,
            @Nullable  List<DayOfWeek> daysOfweek,
            @Nullable List<Integer> daysOfMonth,
            @Nullable List<Integer> weeksOfMonth) {
        RecurrenceApiDTO recurrenceApiDTO = new RecurrenceApiDTO();
        recurrenceApiDTO.setType(type);
        if (interval != null) {
            recurrenceApiDTO.setInterval(interval);
        }
        if (daysOfweek != null) {
            recurrenceApiDTO.setDaysOfWeek(daysOfweek);
        }
        if (daysOfMonth != null) {
            recurrenceApiDTO.setDaysOfMonth(daysOfMonth);
        }
        if (weeksOfMonth != null) {
            recurrenceApiDTO.setWeekOfTheMonth(weeksOfMonth);
        }
        return recurrenceApiDTO;
    }

    private void verifyInputConversionResults(@Nonnull final Schedule schedule) {
        assertEquals(DISPLAY_NAME, schedule.getDisplayName());
        assertEquals(START_TIME_TS, schedule.getStartTime());
        assertEquals(END_TIME_TS, schedule.getEndTime());
        assertEquals(TIMEZONE, schedule.getTimezoneId());
    }

    private void verifyInputConversionLastDate(@Nonnull final Schedule schedule) {
        assertFalse(schedule.hasPerpetual());
        assertEquals(END_DATE_TS, schedule.getLastDate());
    }

    private void verifyInputConversionStartDate(@Nonnull final Schedule schedule) {
        assertTrue(schedule.hasRecurrenceStart());
        assertEquals(START_DATE_TS, schedule.getRecurrenceStart().getRecurrenceStartTime());
    }

    private void verifyScheduleConversionResults(@Nonnull final ScheduleApiDTO schedule) {
        assertEquals(DISPLAY_NAME, schedule.getDisplayName());
        assertEquals(START_TIME, schedule.getStartTime());
        assertEquals(END_TIME, schedule.getEndTime());
        assertEquals(TIMEZONE, schedule.getTimeZone());
    }

    /**
     * Test convert perpetual one time input schedule.
     *
     * @throws Exception If any unexpected exceptions.
     */
    @Test
    public void testConvertInputOneTime() throws Exception {
        ScheduleApiDTO scheduleApiDTO = createScheduleApiDto();
        Schedule convertedSchedule = scheduleMapper.convertInput(scheduleApiDTO);
        verifyInputConversionResults(convertedSchedule);
        assertTrue(convertedSchedule.hasOneTime());
        assertTrue(convertedSchedule.hasPerpetual());
    }

    /**
     * Test convert one time input schedule with last date.
     *
     * @throws Exception If any unexpected exceptions.
     */
    @Test
    public void testConvertInputOneTimeWithLastDate() throws Exception {
        ScheduleApiDTO scheduleApiDTO = createScheduleApiDto();
        scheduleApiDTO.setEndDate(END_DATE);
        Schedule convertedSchedule = scheduleMapper.convertInput(scheduleApiDTO);
        verifyInputConversionResults(convertedSchedule);
        verifyInputConversionLastDate(convertedSchedule);
        assertTrue(convertedSchedule.hasOneTime());
    }

    /**
     * Test convert input schedule with daily recurrence.
     *
     * @throws Exception If any unexpected exceptions.
     */
    @Test
    public void testConvertInputDailyRecurrence() throws Exception {
        ScheduleApiDTO scheduleApiDTO = createScheduleApiDto();
        RecurrenceApiDTO recurrenceApiDTO = createRecurrenceApiDto(RecurrenceType.DAILY, null,
            null, null, null);
        scheduleApiDTO.setRecurrence(recurrenceApiDTO);
        Schedule convertedSchedule = scheduleMapper.convertInput(scheduleApiDTO);
        verifyInputConversionResults(convertedSchedule);
        assertFalse(convertedSchedule.hasOneTime());
        assertEquals("FREQ=DAILY;", convertedSchedule.getRecurRule());

        recurrenceApiDTO = createRecurrenceApiDto(RecurrenceType.DAILY, 2, null,
            null, null);
        scheduleApiDTO.setRecurrence(recurrenceApiDTO);
        convertedSchedule = scheduleMapper.convertInput(scheduleApiDTO);
        verifyInputConversionResults(convertedSchedule);
        assertFalse(convertedSchedule.hasOneTime());
        assertEquals("FREQ=DAILY;INTERVAL=2;", convertedSchedule.getRecurRule());
    }

    /**
     * Test convert input schedule with recurrence start date.
     *
     * @throws Exception If any unexpected exceptions.
     */
    @Test
    public void testConvertInputRecurrenceWithRecurStartDate() throws Exception {
        ScheduleApiDTO scheduleApiDTO = createScheduleApiDto();
        RecurrenceApiDTO recurrenceApiDTO = createRecurrenceApiDto(RecurrenceType.DAILY, 2,
            null, null, null);
        scheduleApiDTO.setRecurrence(recurrenceApiDTO);
        scheduleApiDTO.setStartDate(START_DATE);
        Schedule convertedSchedule = scheduleMapper.convertInput(scheduleApiDTO);
        verifyInputConversionResults(convertedSchedule);
        verifyInputConversionStartDate(convertedSchedule);
        assertFalse(convertedSchedule.hasOneTime());
        assertEquals("FREQ=DAILY;INTERVAL=2;", convertedSchedule.getRecurRule());
    }

    /**
     * Test convert input schedule with weekly recurrence.
     *
     * @throws Exception If any unexpected exceptions.
     */
    @Test
    public void testConvertInputWeeklyRecurrence() throws Exception {
        ScheduleApiDTO scheduleApiDTO = createScheduleApiDto();
        RecurrenceApiDTO recurrenceApiDTO = createRecurrenceApiDto(RecurrenceType.WEEKLY, 1,
            null, null, null);
        scheduleApiDTO.setRecurrence(recurrenceApiDTO);
        Schedule convertedSchedule = scheduleMapper.convertInput(scheduleApiDTO);
        verifyInputConversionResults(convertedSchedule);
        assertFalse(convertedSchedule.hasOneTime());
        assertEquals("FREQ=WEEKLY;BYDAY=TH;INTERVAL=1;", convertedSchedule.getRecurRule());

        recurrenceApiDTO = createRecurrenceApiDto(RecurrenceType.WEEKLY, 1,
            Arrays.asList(DayOfWeek.Sat, DayOfWeek.Sun), null, null);
        scheduleApiDTO.setRecurrence(recurrenceApiDTO);
        convertedSchedule = scheduleMapper.convertInput(scheduleApiDTO);
        verifyInputConversionResults(convertedSchedule);
        assertFalse(convertedSchedule.hasOneTime());
        assertEquals("FREQ=WEEKLY;BYDAY=SA,SU;INTERVAL=1;", convertedSchedule.getRecurRule());
    }

    /**
     * Test convert input schedule with monthly recurrence.
     *
     * @throws Exception If any unexpected exceptions.
     */
    @Test
    public void testConvertInputMonthlyRecurrence() throws Exception {
        ScheduleApiDTO scheduleApiDTO = createScheduleApiDto();
        RecurrenceApiDTO recurrenceApiDTO = createRecurrenceApiDto(RecurrenceType.MONTHLY, 1,
            null, null, null);
        scheduleApiDTO.setRecurrence(recurrenceApiDTO);
        Schedule convertedSchedule = scheduleMapper.convertInput(scheduleApiDTO);
        verifyInputConversionResults(convertedSchedule);
        assertFalse(convertedSchedule.hasOneTime());
        assertEquals("FREQ=MONTHLY;BYMONTHDAY=10;INTERVAL=1;", convertedSchedule.getRecurRule());

        recurrenceApiDTO = createRecurrenceApiDto(RecurrenceType.MONTHLY, 1,
            null, Arrays.asList(5, 10), null);
        scheduleApiDTO.setRecurrence(recurrenceApiDTO);
        convertedSchedule = scheduleMapper.convertInput(scheduleApiDTO);
        verifyInputConversionResults(convertedSchedule);
        assertFalse(convertedSchedule.hasOneTime());
        assertEquals("FREQ=MONTHLY;BYMONTHDAY=5,10;INTERVAL=1;",
            convertedSchedule.getRecurRule());

        recurrenceApiDTO = createRecurrenceApiDto(RecurrenceType.MONTHLY, 1,
            Arrays.asList(DayOfWeek.Mon, DayOfWeek.Tue), null, Arrays.asList(1));
        scheduleApiDTO.setRecurrence(recurrenceApiDTO);
        convertedSchedule = scheduleMapper.convertInput(scheduleApiDTO);
        verifyInputConversionResults(convertedSchedule);
        assertFalse(convertedSchedule.hasOneTime());
        assertEquals("FREQ=MONTHLY;BYDAY=MO,TU;BYSETPOS=1;INTERVAL=1;",
            convertedSchedule.getRecurRule());
    }

    /**
     * Test convert one time schedule.
     *
     * @throws Exception If any unexpected exceptions.
     */
    @Test
    public void testConvertOneTimeSchedule() throws Exception {
        Schedule schedule = Schedule.newBuilder()
            .setId(1L)
            .setDisplayName(DISPLAY_NAME)
            .setStartTime(START_TIME_TS)
            .setEndTime(END_TIME_TS)
            .setTimezoneId(TIMEZONE)
            .setOneTime(OneTime.getDefaultInstance())
            .setNextOccurrence(NextOccurrence.newBuilder()
                .setStartTime(NEXT_OCCURRENCE_TIMESTAMP)
                .build())
            .setActive(Active.newBuilder()
                .setRemainingActiveTimeMs(REMAINING_TIME_ACTIVE)
                .build())
            .build();
        ScheduleApiDTO scheduleApiDTO = scheduleMapper.convertSchedule(schedule);
        verifyScheduleConversionResults(scheduleApiDTO);
        assertTrue(schedule.hasNextOccurrence());
        assertEquals(NEXT_OCCURRENCE, scheduleApiDTO.getNextOccurrence());
        assertEquals(NEXT_OCCURRENCE_TIMESTAMP,
            scheduleApiDTO.getNextOccurrenceTimestamp().longValue());
    }

    /**
     * Test convert schedule with start date.
     *
     * @throws Exception If any unexpected exceptions.
     */
    @Test
    public void testConvertRecurScheduleWithStartDate() throws Exception {
        Schedule schedule = Schedule.newBuilder()
            .setId(1L)
            .setDisplayName(DISPLAY_NAME)
            .setStartTime(START_TIME_TS)
            .setEndTime(END_TIME_TS)
            .setRecurrenceStart(RecurrenceStart.newBuilder()
                .setRecurrenceStartTime(START_DATE_TS)
                .build())
            .setTimezoneId(TIMEZONE)
            .setRecurRule("FREQ=DAILY;")
            .build();
        ScheduleApiDTO scheduleApiDTO = scheduleMapper.convertSchedule(schedule);
        verifyScheduleConversionResults(scheduleApiDTO);
        assertEquals(START_DATE, scheduleApiDTO.getStartDate());
    }

    /**
     * Test convert schedule with daily recurrence.
     *
     * @throws Exception If any unexpected exceptions.
     */
    @Test
    public void testConvertDailyRecurSchedule() throws Exception {
        Schedule schedule = Schedule.newBuilder()
            .setId(1L)
            .setDisplayName(DISPLAY_NAME)
            .setStartTime(START_TIME_TS)
            .setEndTime(END_TIME_TS)
            .setTimezoneId(TIMEZONE)
            .setRecurRule("FREQ=DAILY;INTERVAL=2")
            .build();
        ScheduleApiDTO scheduleApiDTO = scheduleMapper.convertSchedule(schedule);
        verifyScheduleConversionResults(scheduleApiDTO);
        assertNotNull(scheduleApiDTO.getRecurrence());
        assertEquals(RecurrenceType.DAILY, scheduleApiDTO.getRecurrence().getType());
        assertEquals(2, scheduleApiDTO.getRecurrence().getInterval().intValue());
    }

    /**
     * Test convert schedule with weekly recurrence.
     *
     * @throws Exception If any unexpected exceptions.
     */
    @Test
    public void testConvertWeeklyRecurSchedule() throws Exception {
        Schedule schedule = Schedule.newBuilder()
            .setId(1L)
            .setDisplayName(DISPLAY_NAME)
            .setStartTime(START_TIME_TS)
            .setEndTime(END_TIME_TS)
            .setTimezoneId(TIMEZONE)
            .setRecurRule("FREQ=WEEKLY;BYDAY=SA,SU;INTERVAL=1;")
            .build();
        ScheduleApiDTO scheduleApiDTO = scheduleMapper.convertSchedule(schedule);
        verifyScheduleConversionResults(scheduleApiDTO);
        RecurrenceApiDTO recurrenceApiDTO = scheduleApiDTO.getRecurrence();
        assertNotNull(recurrenceApiDTO);
        assertEquals(RecurrenceType.WEEKLY, recurrenceApiDTO.getType());
        assertEquals(1, scheduleApiDTO.getRecurrence().getInterval().intValue());
        assertNotNull(recurrenceApiDTO.getDaysOfWeek());
        assertEquals(2, recurrenceApiDTO.getDaysOfWeek().size());
    }

    /**
     * Test convert schedule with weekly recurrence.
     *
     * @throws Exception If any unexpected exceptions.
     */
    @Test
    public void testConvertMonthlyRecurSchedule() throws Exception {
        Schedule schedule = Schedule.newBuilder()
            .setId(1L)
            .setDisplayName(DISPLAY_NAME)
            .setStartTime(START_TIME_TS)
            .setEndTime(END_TIME_TS)
            .setTimezoneId(TIMEZONE)
            .setRecurRule("FREQ=MONTHLY;BYMONTHDAY=5,10;INTERVAL=1;")
            .build();
        ScheduleApiDTO scheduleApiDTO = scheduleMapper.convertSchedule(schedule);
        verifyScheduleConversionResults(scheduleApiDTO);
        RecurrenceApiDTO recurrenceApiDTO = scheduleApiDTO.getRecurrence();
        assertNotNull(recurrenceApiDTO);
        assertEquals(RecurrenceType.MONTHLY, recurrenceApiDTO.getType());
        assertEquals(1, scheduleApiDTO.getRecurrence().getInterval().intValue());
        assertNotNull(recurrenceApiDTO.getDaysOfMonth());
        assertEquals(2, recurrenceApiDTO.getDaysOfMonth().size());

        schedule = schedule.toBuilder()
            .setRecurRule("FREQ=MONTHLY;BYDAY=MO,TU;BYSETPOS=1;INTERVAL=1;")
            .build();
        scheduleApiDTO = scheduleMapper.convertSchedule(schedule);
        verifyScheduleConversionResults(scheduleApiDTO);
        recurrenceApiDTO = scheduleApiDTO.getRecurrence();
        assertNotNull(recurrenceApiDTO);
        assertEquals(RecurrenceType.MONTHLY, recurrenceApiDTO.getType());
        assertEquals(1, scheduleApiDTO.getRecurrence().getInterval().intValue());
        assertNotNull(recurrenceApiDTO.getWeekOfTheMonth());
        assertEquals(1, recurrenceApiDTO.getWeekOfTheMonth().size());
        assertNotNull(recurrenceApiDTO.getDaysOfWeek());
        assertEquals(2, recurrenceApiDTO.getDaysOfWeek().size());
    }

    /**
     * Test convert schedules.
     *
     * @throws Exception If any unexpected exceptions.
     */
    @Test
    public void testConvertSchedules() throws Exception {
        Schedule schedule = Schedule.newBuilder()
            .setId(1L)
            .setDisplayName(DISPLAY_NAME)
            .setStartTime(START_TIME_TS)
            .setEndTime(END_TIME_TS)
            .setTimezoneId(TIMEZONE)
            .setOneTime(OneTime.getDefaultInstance())
            .setNextOccurrence(NextOccurrence.newBuilder()
                .setStartTime(NEXT_OCCURRENCE_TIMESTAMP)
                .build())
            .setActive(Active.newBuilder()
                .setRemainingActiveTimeMs(REMAINING_TIME_ACTIVE)
                .build())
            .build();
        List<ScheduleApiDTO> scheduleApiDTOs = scheduleMapper.convertSchedules(Arrays.asList(schedule));
        assertEquals(1, scheduleApiDTOs.size());
        ScheduleApiDTO scheduleApiDTO = scheduleApiDTOs.get(0);
        verifyScheduleConversionResults(scheduleApiDTO);
        assertTrue(schedule.hasNextOccurrence());
        assertEquals(NEXT_OCCURRENCE, scheduleApiDTO.getNextOccurrence());
        assertEquals(NEXT_OCCURRENCE_TIMESTAMP,
            scheduleApiDTO.getNextOccurrenceTimestamp().longValue());
    }

    /**
     * Test convert schedule with invalid recurrence.
     *
     * @throws Exception If any unexpected exceptions.
     */
    @Test
    public void testConvertScheduleInvalidRecurRule() throws Exception {
        Schedule schedule = Schedule.newBuilder()
            .setId(1L)
            .setDisplayName(DISPLAY_NAME)
            .setStartTime(START_TIME_TS)
            .setEndTime(END_TIME_TS)
            .setTimezoneId(TIMEZONE)
            .setRecurRule("UNTIL=INVALID;")
            .build();
        thrown.expect(OperationFailedException.class);
        scheduleMapper.convertSchedule(schedule);
    }


    @Nonnull
    private LocalDateTime toLocalDateTime(final String dateTime, @Nonnull final TimeZone timeZone) {
        return Instant.ofEpochMilli(DateTimeUtil.parseIso8601TimeAndAdjustTimezone(dateTime, timeZone))
            .atZone(timeZone.toZoneId()).toLocalDateTime();
    }

    @Nonnull
    private LocalDate toLocalDate(final String dateTime, @Nonnull final TimeZone timeZone) {
        return Instant.ofEpochMilli(DateTimeUtil.parseIso8601TimeAndAdjustTimezone(dateTime, timeZone))
            .atZone(timeZone.toZoneId()).toLocalDate();
    }
}
