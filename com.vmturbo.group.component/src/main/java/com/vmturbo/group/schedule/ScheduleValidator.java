package com.vmturbo.group.schedule;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule;
import com.vmturbo.group.common.InvalidItemException;

/**
 * An interface to abstract away the validation of schedules in order
 * to make unit testing easier.
 */
@FunctionalInterface
public interface ScheduleValidator {
    /**
     * Validate schedule.
     *
     * @param schedule Schedule to validate
     * @throws InvalidItemException If schedule is invalid
     */
    void validateSchedule(@Nonnull Schedule schedule) throws InvalidItemException;
}
