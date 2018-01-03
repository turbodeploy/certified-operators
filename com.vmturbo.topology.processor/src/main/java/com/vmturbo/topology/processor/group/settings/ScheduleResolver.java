package com.vmturbo.topology.processor.group.settings;

import static java.time.ZoneOffset.UTC;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.Schedule;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule.DayOfWeek;

/**
 * ScheduleResolver - used to determine if a Schedule (as used in a settings policy or other
 * scheduled item) is applicable at the resolution moment, currently the moment that the
 * ScheduleResolver is created.
 *
 * Currently this class is only used during entity settings resolution, but should the Schedule
 * structure end up in wider use than just in the SettingsPolicy, this class may need to move
 */
public class ScheduleResolver {

    private final Instant resolutionInstant;

    public ScheduleResolver(@Nonnull final Instant resolutionInstant) {
        this.resolutionInstant = resolutionInstant;
    }

    /**
     * Determines whether a Schedule applies at this ScheduleResolver's resolutionInstant.
     * @param schedule the Schedule to test
     * @return true if the Schedule applies at the resolutionInstant and false if it does not.
     */
    public boolean appliesAtResolutionInstant(@Nonnull Schedule schedule) {
        return this.appliesAtInstant(resolutionInstant, schedule);
    }

    /**
     * Determines whether a Schedule applies at an Instant.
     * @param instant the Instant to test
     * @param schedule the Schedule to test
     * @return true if the Schedule applies at the Instant and false if it does not.
     */
    private boolean appliesAtInstant(Instant instant, @Nonnull Schedule schedule) {

        final LocalTime time = instant.atOffset(UTC).toLocalTime();
        final LocalDate date = instant.atOffset(UTC).toLocalDate();
        final DayOfWeek dayOfWeek = Schedule.DayOfWeek.valueOf(date.getDayOfWeek().name());

        final LocalDate startDate = Instant.ofEpochMilli(schedule.getStartTime())
                .atOffset(UTC).toLocalDate();
        final LocalTime startTime = Instant.ofEpochMilli(schedule.getStartTime())
                .atOffset(UTC).toLocalTime();
        final LocalTime endTime = schedule.hasMinutes() ? startTime.plusMinutes(schedule.getMinutes())
                : Instant.ofEpochMilli(schedule.getEndTime()).atOffset(UTC).toLocalTime();


        final boolean timeFits = !time.isAfter(endTime) &&
                !time.isBefore(startTime);
        final boolean dateFitsBeginning = !date.isBefore(startDate);


        final boolean dateFitsEnding = schedule.hasOneTime() ? !date.isAfter(startDate) :
                (schedule.hasPerpetual() || !date.isAfter(Instant.ofEpochMilli(
                        schedule.getLastDate()).atOffset(UTC).toLocalDate()));

        final boolean timeAndDateFits = timeFits && dateFitsBeginning && dateFitsEnding;

        final boolean weekDayFitsIfNecessary = !schedule.hasWeekly() ||
                schedule.getWeekly().getDaysOfWeekList().contains(dayOfWeek);
        final boolean monthDayFitsIfNecessary = !schedule.hasMonthly() ||
                schedule.getMonthly().getDaysOfMonthList().contains(date.getDayOfMonth());

        return timeAndDateFits && weekDayFitsIfNecessary && monthDayFitsIfNecessary;
    }
}
