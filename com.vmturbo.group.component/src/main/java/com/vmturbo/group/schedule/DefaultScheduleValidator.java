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
            errors.add("Schedule must have a name");
        }
        if (schedule.getStartTime() == 0) {
            errors.add("Schedule must have valid start time");
        }
        if (schedule.getEndTime() == 0) {
            errors.add("Schedule must have valid end time");
        }
        if (Strings.isBlank(schedule.getTimezoneId())) {
            errors.add("Schedule must have a timezone ID");
        }
        if (schedule.getLastDate() == 0 && !schedule.hasPerpetual()) {
            errors.add("Schedule must have either valid last date or be perpetual");
        }
        if (!errors.isEmpty()) {
            throw new InvalidItemException(
                "Invalid schedule: " + schedule.getDisplayName() +
                    System.lineSeparator() +
                    StringUtils.join(errors, System.lineSeparator()));
        }
    }
}
