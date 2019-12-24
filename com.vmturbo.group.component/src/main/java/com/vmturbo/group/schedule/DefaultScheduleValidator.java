package com.vmturbo.group.schedule;

import java.util.LinkedList;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;

import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule;
import com.vmturbo.group.common.InvalidItemException;

/**
 * Default implementation of ScheduleValidator.
 */
public class DefaultScheduleValidator implements ScheduleValidator {
    /**
     * {@inheritDoc}
     */
    @Override
    public void validateSchedule(@Nonnull final Schedule schedule) throws InvalidItemException {
        final List<String> errors = new LinkedList<>();
        if (Strings.isBlank(schedule.getDisplayName())) {
            errors.add("Schedule must have a name.");
        }
        if (!schedule.hasStartTime()) {
            errors.add("Schedule must have a valid start time.");
        }
        if (!schedule.hasEndTime()) {
            errors.add("Schedule must have a valid end time.");
        }
        if (schedule.hasEndTime() && (schedule.getStartTime() >= schedule.getEndTime())) {
            errors.add("Schedule end time must be after start time.");
        }
        if (Strings.isBlank(schedule.getTimezoneId())) {
            errors.add("Schedule must have a timezone ID.");
        }
        if (schedule.hasOneTime() && schedule.hasLastDate()) {
            errors.add("One time schedule can not have end date.");
        }
        if (!schedule.hasOneTime() && !schedule.hasRecurRule()) {
            errors.add("Schedule must be either one time or recurring.");
        }
        if (!schedule.hasOneTime() && !(schedule.hasLastDate() || schedule.hasPerpetual())) {
            errors.add("Recurring schedule must have either a valid last date or be perpetual.");
        }
        if (schedule.hasLastDate() && schedule.getStartTime() > schedule.getLastDate()) {
            errors.add("Last date of recurring schedule must be after start time.");
        }

        if (!errors.isEmpty()) {
            throw new InvalidItemException(
                "Invalid schedule: " + schedule.getDisplayName() +
                    System.lineSeparator() +
                    StringUtils.join(errors, System.lineSeparator()));
        }
    }
}
