package com.vmturbo.group.schedule;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.OneTime;
import com.vmturbo.group.common.InvalidItemException;

/**
 * {@link DefaultScheduleValidator} unit tests.
 */
public class DefaultScheduleValidatorTest {

    private final DefaultScheduleValidator validator = new DefaultScheduleValidator();

    /**
     * Expected exception.
     */
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    /**
     * Test validate schedule name.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void testValidateName() throws Exception {
        Schedule schedule = Schedule.getDefaultInstance();
        expectedException.expect(InvalidItemException.class);
        expectedException.expectMessage("Schedule must have a name");
        validator.validateSchedule(schedule);
    }

    /**
     * Test validate schedule start time.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void testValidateStartTime() throws Exception {
        Schedule schedule = Schedule.newBuilder().setDisplayName("test").build();
        expectedException.expect(InvalidItemException.class);
        expectedException.expectMessage("Schedule must have a valid start time");
        validator.validateSchedule(schedule);
    }

    /**
     * Test validate schedule end time.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void testValidateEndTime() throws Exception {
        Schedule schedule = Schedule.newBuilder().setDisplayName("test")
            .setStartTime(1111L)
            .build();
        expectedException.expect(InvalidItemException.class);
        expectedException.expectMessage("Schedule must have a valid end time");
        validator.validateSchedule(schedule);
    }

    /**
     * Test validate schedule times order.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void testValidateCorrectTimeOrder() throws Exception {
        Schedule schedule = Schedule.newBuilder().setDisplayName("test")
            .setStartTime(22L)
            .setEndTime(11L)
            .build();
        expectedException.expect(InvalidItemException.class);
        expectedException.expectMessage("Schedule end time must be after start time.");
        validator.validateSchedule(schedule);
    }

    /**
     * Test validate schedule tomezone.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void testValidateTimezone() throws Exception {
        Schedule schedule = Schedule.newBuilder().setDisplayName("test")
            .setStartTime(11L)
            .setEndTime(22L)
            .build();
        expectedException.expect(InvalidItemException.class);
        expectedException.expectMessage("Schedule must have a timezone ID.");
        validator.validateSchedule(schedule);
    }

    /**
     * Test validate schedule recurrence is set.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void testValidateRecurrence() throws Exception {
        Schedule schedule = Schedule.newBuilder().setDisplayName("test")
            .setStartTime(11L)
            .setEndTime(22L)
            .setTimezoneId("test_timezone")
            .build();
        expectedException.expect(InvalidItemException.class);
        expectedException.expectMessage("Schedule must be either one time or recurring.");
        validator.validateSchedule(schedule);
    }

    /**
     * Test validate recurring schedule is perpetual or has proper end date.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void testValidateRecurringLastDateOrPerpetual() throws Exception {
        Schedule schedule = Schedule.newBuilder().setDisplayName("test")
            .setStartTime(11L)
            .setEndTime(22L)
            .setTimezoneId("test_timezone")
            .setRecurRule("rrule")
            .build();
        expectedException.expect(InvalidItemException.class);
        expectedException.expectMessage("Recurring schedule must have either a valid last date or be perpetual.");
        validator.validateSchedule(schedule);
    }

    /**
     * Test validate last date is set correctly.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void testValidateRecurringLastDate() throws Exception {
        Schedule schedule = Schedule.newBuilder().setDisplayName("test")
            .setStartTime(33L)
            .setEndTime(22L)
            .setTimezoneId("test_timezone")
            .setLastDate(11L)
            .setRecurRule("rrule")
            .build();
        expectedException.expect(InvalidItemException.class);
        expectedException.expectMessage("Last date of recurring schedule must be after start time.");
        validator.validateSchedule(schedule);
    }

    /**
     * Test validate one time schedule cannot have last date.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void testValidateOneTimeLastDate() throws Exception {
        Schedule schedule = Schedule.newBuilder().setDisplayName("test")
            .setStartTime(11L)
            .setEndTime(22L)
            .setTimezoneId("test_timezone")
            .setLastDate(33L)
            .setOneTime(OneTime.getDefaultInstance())
            .build();
        expectedException.expect(InvalidItemException.class);
        expectedException.expectMessage("One time schedule can not have end date.");
        validator.validateSchedule(schedule);
    }

}
