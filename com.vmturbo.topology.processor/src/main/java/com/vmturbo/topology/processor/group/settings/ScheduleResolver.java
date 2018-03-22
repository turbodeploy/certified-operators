package com.vmturbo.topology.processor.group.settings;

import static java.time.ZoneOffset.UTC;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Optional;

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
        final LocalDate endDate = schedule.hasMinutes() ? Instant.ofEpochMilli(schedule.getStartTime())
                .atOffset(UTC).plusMinutes(schedule.getMinutes()).toLocalDate()
                : Instant.ofEpochMilli(schedule.getEndTime()).atOffset(UTC).toLocalDate();
        final LocalTime endTime = schedule.hasMinutes() ? startTime.plusMinutes(schedule.getMinutes())
                : Instant.ofEpochMilli(schedule.getEndTime()).atOffset(UTC).toLocalTime();
        final Optional<LocalDate> lastDate = schedule.hasPerpetual() ? Optional.empty()
                : Optional.of(Instant.ofEpochMilli(schedule.getLastDate()).atOffset(UTC).toLocalDate());

        final boolean timeFits = checkIfTimeFits(time, date, startTime, startDate, endTime, endDate, lastDate);

        final boolean dateFitsBeginning = !date.isBefore(startDate);

        final boolean dateFitsEnding = schedule.hasOneTime() ? !date.isAfter(endDate) :
                (!lastDate.isPresent() || !date.isAfter(lastDate.get()));

        final boolean timeAndDateFits = timeFits && dateFitsBeginning && dateFitsEnding;

        final boolean weekDayFitsIfNecessary = !schedule.hasWeekly() ||
                schedule.getWeekly().getDaysOfWeekList().contains(dayOfWeek);
        final boolean monthDayFitsIfNecessary = !schedule.hasMonthly() ||
                schedule.getMonthly().getDaysOfMonthList().contains(date.getDayOfMonth());

        return timeAndDateFits && weekDayFitsIfNecessary && monthDayFitsIfNecessary;
    }

    /**
     * Check if current local date and time is in the range of startTime and endTime. And there is a
     * edge case that endTime could be the next day's time. It needs to compare start date with end
     * date first to see if start time and end time is at same day or not. And if start date is
     * different with end date, that means end date is be the next day, because we already validate
     * that end time is larger than start time.
     * <p>
     * For example: start date is 2018-01-10, and
     *  ------start time(20:00PM) ----|next day| ----end time(2:00AM) ---
     *  And if it is one time schedule, and only when current date time is between the time range of
     *  2018-01-10/20:00 PM and 2018-01-11/02:00 AM, then the schedule will be active.
     *  And if it is recurrence, when current time is between time 20:00 PM - next day 02:00 AM and
     *  date is any day valid (from start date to last effective day), then the schedule will be active.
     *
     * @param currentTime current local time.
     * @param currentDate current local date.
     * @param startTime schedule start local time.
     * @param startDate schedule start local date.
     * @param endTime schedule end local time.
     * @param endDate schedule end local date which is different with last date. End date is combine
     *                with end time to describe the effective end time range within a day.
     * @param lastDate schedule last effective date, note that last date is different with end date,
     *                last date means the schedule expire date. After last date, schedule will expire.
     * @return true if current time is in the range of startTime and endTime, otherwise return false.
     */
    private boolean checkIfTimeFits(@Nonnull final LocalTime currentTime,
                                    @Nonnull final LocalDate currentDate,
                                    @Nonnull final LocalTime startTime,
                                    @Nonnull final LocalDate startDate,
                                    @Nonnull final LocalTime endTime,
                                    @Nonnull final LocalDate endDate,
                                    @Nonnull final Optional<LocalDate> lastDate) {
        // if startTime and endTime are on the same day, we can directly compare current time
        // with start time and end time.
        if (endDate.isEqual(startDate)) {
            return !currentTime.isBefore(startTime) && !currentTime.isAfter(endTime);
        } else {
            // in this case, endTime will be next day's time. For example, start time is today afternoon,
            // endTime is tomorrow morning. And also currentDate should be larger or equal to start
            // date and smaller or equal to last date.
            // We can separate the day ranges into three parts: start date, days between start date
            // with end date, end date. For different part, the valid time range is different.
            boolean isTimeFits = false;
            // if current date is same as start date, the valid time range is only from startTime to 12:00AM
            if (currentDate.isEqual(startDate)) {
                isTimeFits |= !currentTime.isBefore(startTime);
            }
            else if (lastDate.isPresent() && currentDate.isEqual(lastDate.get())) {
                // if schedule has last date, and current date is equal to last day, the valid time
                // range is only from 12:00 AM to endTime
                isTimeFits |= !currentTime.isAfter(endTime);
            }
            else if (!lastDate.isPresent() || (lastDate.isPresent() && currentDate.isAfter(startDate)
                // if current date is between start date with end date, the valid time range are two
                // parts: 12:00 AM to endTime and startTime to 12:00 AM, either one is valid time.
                // if policy has no last day, it is same as end date which is infinite, it also
                // belongs to this check.
                    && currentDate.isBefore(lastDate.get()))) {
                isTimeFits |= !currentTime.isBefore(startTime) || !currentTime.isAfter(endTime);
            }
            return isTimeFits;
        }
    }
}
