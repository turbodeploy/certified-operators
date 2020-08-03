package com.vmturbo.api.component.external.api.mapper;

import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collection;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableBiMap;

import jersey.repackaged.com.google.common.collect.Lists;

import net.fortuna.ical4j.model.Recur.Frequency;
import net.fortuna.ical4j.model.WeekDay;
import net.fortuna.ical4j.model.property.RRule;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.settingspolicy.RecurrenceApiDTO;
import com.vmturbo.api.dto.settingspolicy.ScheduleApiDTO;
import com.vmturbo.api.enums.DayOfWeek;
import com.vmturbo.api.enums.RecurrenceType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.OneTime;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.Perpetual;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.RecurrenceStart;

/**
 * Responsible for mapping Schedule XL objects to their API counterparts.
 */
public class ScheduleMapper {
    private static final String UNSUPPORTED_RECURRENCE_TYPE_MESSAGE =
        "Unsupported recurrence type %s. Only WEEKLY, MONTHLY, and DAILY recurrence is supported.";
    private static final ImmutableBiMap<DayOfWeek, WeekDay> weekdays = ImmutableBiMap
        .<DayOfWeek, WeekDay>builder()
        .put(DayOfWeek.Mon, WeekDay.MO)
        .put(DayOfWeek.Tue, WeekDay.TU)
        .put(DayOfWeek.Wed, WeekDay.WE)
        .put(DayOfWeek.Thu, WeekDay.TH)
        .put(DayOfWeek.Fri, WeekDay.FR)
        .put(DayOfWeek.Sat, WeekDay.SA)
        .put(DayOfWeek.Sun, WeekDay.SU)
        .build();

    private static final ImmutableBiMap<java.time.DayOfWeek, WeekDay> localWeekDays = ImmutableBiMap
        .<java.time.DayOfWeek, WeekDay>builder()
        .put(java.time.DayOfWeek.MONDAY, WeekDay.MO)
        .put(java.time.DayOfWeek.TUESDAY, WeekDay.TU)
        .put(java.time.DayOfWeek.WEDNESDAY, WeekDay.WE)
        .put(java.time.DayOfWeek.THURSDAY, WeekDay.TH)
        .put(java.time.DayOfWeek.FRIDAY, WeekDay.FR)
        .put(java.time.DayOfWeek.SATURDAY, WeekDay.SA)
        .put(java.time.DayOfWeek.SUNDAY, WeekDay.SU)
        .build();

    private final Logger logger = LogManager.getLogger();

    /**
     * Convert from Schedule API input to XL schedule object.
     *
     * @param scheduleApiDTO Schedule API input.
     * @return XL schedule object.
     * @throws OperationFailedException If exception is thrown during conversion.
     */
    @Nonnull
    public Schedule convertInput(@Nonnull final ScheduleApiDTO scheduleApiDTO)
        throws OperationFailedException {

        final TimeZone timeZone = DateTimeUtil.getTimeZone(scheduleApiDTO.getTimeZone());
        final Schedule.Builder schedule = Schedule.newBuilder()
            .setDisplayName(scheduleApiDTO.getDisplayName())
            .setTimezoneId(timeZone.getID())
            .setStartTime(scheduleApiDTO.getStartTime().atZone(timeZone.toZoneId()).toInstant()
                .toEpochMilli())
            .setEndTime(scheduleApiDTO.getEndTime().atZone(timeZone.toZoneId()).toInstant()
                .toEpochMilli());
        // deferred recurrence start date
        if (scheduleApiDTO.getStartDate() != null) {
            schedule.setRecurrenceStart(RecurrenceStart.newBuilder()
                .setRecurrenceStartTime(scheduleApiDTO.getStartDate().atZone(timeZone.toZoneId())
                    .toInstant().toEpochMilli())
                .build());
        }
        if (scheduleApiDTO.getEndDate() != null) {
            final long lastDateTime = (scheduleApiDTO.getEndDate().atTime(LocalTime.MAX)
                .atZone(timeZone.toZoneId()).toInstant().toEpochMilli());
            schedule.setLastDate(lastDateTime);
        } else {
            schedule.setPerpetual(Perpetual.getDefaultInstance());
        }
        // Recurrence uses the iCal library: http://www.kanzaki.com/docs/ical/rrule.html
        if (scheduleApiDTO.getRecurrence() != null) {
            schedule.setRecurRule(getRecurrenceFromInput(scheduleApiDTO));
        } else {
            schedule.setOneTime(OneTime.getDefaultInstance());
        }
        return schedule.build();
    }

    /**
     * Convert from XL schedule object to Schedule API.
     *
     * @param schedule XL schedule object.
     * @return Schedule API object.
     * @throws OperationFailedException If exception is thrown during conversion.
     */
    @Nonnull
    public ScheduleApiDTO convertSchedule(@Nonnull final Schedule schedule)
        throws OperationFailedException {
        final ScheduleApiDTO scheduleApiDto = new ScheduleApiDTO();
        scheduleApiDto.setUuid(Long.toString(schedule.getId()));
        scheduleApiDto.setDisplayName(schedule.getDisplayName());
        final TimeZone timeZone = TimeZone.getTimeZone(schedule.getTimezoneId());
        scheduleApiDto.setTimeZone(schedule.getTimezoneId());
        scheduleApiDto.setStartTime(LocalDateTime.ofInstant(
            Instant.ofEpochMilli(schedule.getStartTime()), timeZone.toZoneId()));
        scheduleApiDto.setEndTime(LocalDateTime.ofInstant(
            Instant.ofEpochMilli(schedule.getEndTime()), timeZone.toZoneId()));
        if (schedule.hasRecurrenceStart()) {
            scheduleApiDto.setStartDate(LocalDateTime.ofInstant(
                Instant.ofEpochMilli(schedule.getRecurrenceStart().getRecurrenceStartTime()),
                timeZone.toZoneId()));
        }
        if (!schedule.hasPerpetual()) {
            scheduleApiDto.setEndDate(LocalDateTime.ofInstant(
                Instant.ofEpochMilli(schedule.getLastDate()), timeZone.toZoneId()).toLocalDate());
        }
        if (schedule.hasNextOccurrence()) {
            final long nextOccurrence = schedule.getNextOccurrence().getStartTime();
            scheduleApiDto.setNextOccurrenceTimestamp(nextOccurrence);
            scheduleApiDto.setNextOccurrence(DateTimeUtil.toString(nextOccurrence, timeZone));
        }
        if (schedule.hasActive()) {
            scheduleApiDto.setRemaingTimeActiveInMs(schedule.getActive().getRemainingActiveTimeMs());
        }
        if (!schedule.hasOneTime()) {
            scheduleApiDto.setRecurrence(getRecurrenceFromSchedule(schedule));
        }
        return scheduleApiDto;
    }

    /**
     * Convert collection of XL schedule objects to their API counterparts.
     *
     * @param schedules Collection of XL schedules.
     * @return Converted Schedule API objects.
     * @throws OperationFailedException If exception is thrown during conversion.
     */
    public List<ScheduleApiDTO> convertSchedules(@Nonnull final Collection<Schedule> schedules)
        throws OperationFailedException {
        final List<ScheduleApiDTO> list = Lists.newArrayList();
        for (final Schedule schedule : schedules) {
            list.add(convertSchedule(schedule));
        }
        return list;
    }

    @Nonnull
    private String getRecurrenceFromInput(@Nonnull final ScheduleApiDTO scheduleApiDto)
        throws OperationFailedException {
        final RecurrenceApiDTO recurrenceDto = scheduleApiDto.getRecurrence();
        final StringBuilder recurrenceQuery = new StringBuilder("FREQ=");
        switch (recurrenceDto.getType()) {
            case DAILY:
                recurrenceQuery.append(Frequency.DAILY);
                break;
            case WEEKLY:
                recurrenceQuery.append(Frequency.WEEKLY);
                recurrenceQuery.append(";BYDAY=");
                final java.time.DayOfWeek dayOfWeek = scheduleApiDto.getStartTime().toLocalDate()
                    .getDayOfWeek();
                final String scheduleStartDay = localWeekDays.get(dayOfWeek).getDay().name();
                if (recurrenceDto.getDaysOfWeek() != null && !recurrenceDto.getDaysOfWeek().isEmpty()) {
                    recurrenceQuery.append(recurrenceDto.getDaysOfWeek().stream()
                        .map(d -> weekdays.get(d).getDay()).filter(d -> d != null)
                        .map(Enum::name)
                        .collect(Collectors.joining(",")));
                } else {
                    // if day of week is unspecified, set it based on start time
                    recurrenceQuery.append(scheduleStartDay);
                }
                // set week start day based on schedule start time, so that the next weekly occurrence
                // will be calculated relative to schedule start time.
                // Otherwise, week start day is based on default value Monday, which may cause
                // unexpected results in case of the recurrence interval greater then 1.
                // e.g. if bi-weekly schedule which recurs on Tuesday every other week starts
                // on Friday Jan 10 and by default week starts on Monday Jan 6, the next scheduled
                // occurrence will be Jan 21 (and not Jan 14 as a customer presumably would expect),
                // because the fist occurrence of the event would be assumed by ical to have happened
                // on Jan 7, after week start day but before schedule start time.
                // NOTE: This only applies to weekly schedules with interval > 1
                // For more details https://tools.ietf.org/html/rfc5545#section-3.3.10
                recurrenceQuery.append(";WKST=").append(scheduleStartDay);
                break;
            case MONTHLY:
                recurrenceQuery.append(Frequency.MONTHLY);
                if (recurrenceDto.getDaysOfMonth() != null && !recurrenceDto.getDaysOfMonth().isEmpty()) {
                    recurrenceQuery.append(";BYMONTHDAY=");
                    recurrenceQuery.append(recurrenceDto.getDaysOfMonth().stream()
                        .map(d -> String.valueOf(d))
                        .collect(Collectors.joining(",")));
                } else if (recurrenceDto.getWeekOfTheMonth() != null
                    && recurrenceDto.getDaysOfWeek() != null
                    && !recurrenceDto.getDaysOfWeek().isEmpty()) {
                    recurrenceQuery.append(";BYDAY=");
                    recurrenceQuery.append(recurrenceDto.getDaysOfWeek().stream()
                        .map(d -> weekdays.get(d).getDay()).filter(d -> d != null)
                        .map(Enum::name)
                        .collect(Collectors.joining(",")));
                    recurrenceQuery.append(";BYSETPOS=");
                    recurrenceQuery.append(recurrenceDto.getWeekOfTheMonth().stream()
                        .map(String::valueOf).collect(Collectors.joining(",")));

                } else {
                    // if neither day nor week of month is specified, set it based on start time
                    final int dayOfMonth = scheduleApiDto.getStartTime().toLocalDate().getDayOfMonth();
                    recurrenceQuery.append(";BYMONTHDAY=").append(dayOfMonth);
                }
                break;
            default:
                logger.error("Unsupported input recurrence type {}",
                    () -> recurrenceDto.getType().name());
                throw new OperationFailedException(String.format(UNSUPPORTED_RECURRENCE_TYPE_MESSAGE,
                    recurrenceDto.getType().name()));

        }
        if (recurrenceDto.getInterval() != null) {
            recurrenceQuery.append(";INTERVAL=");
            recurrenceQuery.append(recurrenceDto.getInterval());
        }
        recurrenceQuery.append(";");
        return recurrenceQuery.toString();
    }

    @Nonnull
    private RecurrenceApiDTO getRecurrenceFromSchedule(@Nonnull final Schedule schedule)
        throws OperationFailedException {
        final RecurrenceApiDTO recurrenceDto = new RecurrenceApiDTO();
        String recurrence = schedule.getRecurRule();
        RRule rule = null;

        try {
            rule = new RRule(recurrence);
        } catch (ParseException e) {
            logger.error("Exception parsing recurrence pattern {} for schedule {} ",
                () -> recurrence, schedule::toString);
            throw new OperationFailedException("Exception parsing recurrence pattern " + recurrence);
        }

        recurrenceDto.setType(RecurrenceType.valueOf(rule.getRecur().getFrequency().name()));
        switch (recurrenceDto.getType()) {
            case WEEKLY:
                recurrenceDto.setDaysOfWeek(getDaysOfWeek(rule));
                break;
            case MONTHLY:
                List<Integer> monthDayList = rule.getRecur().getMonthDayList();
                if (!CollectionUtils.isEmpty(monthDayList)) {
                    recurrenceDto.setDaysOfMonth(monthDayList);
                } else {
                    recurrenceDto.setDaysOfWeek(getDaysOfWeek(rule));
                    if (!CollectionUtils.isEmpty(rule.getRecur().getSetPosList())) {
                        recurrenceDto.setWeekOfTheMonth(
                            rule.getRecur().getSetPosList());
                    }
                }
                break;
            case DAILY:
                break;
            default:
                logger.error("Unsupported recurrence type {} for schedule {}",
                    () -> recurrenceDto.getType().name(), schedule::toString);
                throw new OperationFailedException(String.format(UNSUPPORTED_RECURRENCE_TYPE_MESSAGE,
                    recurrenceDto.getType().name()));
        }

        if (rule.getRecur().getInterval() != -1) {
            recurrenceDto.setInterval(rule.getRecur().getInterval());
        }

        return recurrenceDto;
    }

    @Nonnull
    private List<DayOfWeek> getDaysOfWeek(RRule rule) {
        @SuppressWarnings("unchecked")
        List<WeekDay> dayList = rule.getRecur().getDayList();
        return dayList
            .stream()
            .map(d -> weekdays.inverse().get(d))
            .collect(Collectors.toList());
    }

}
