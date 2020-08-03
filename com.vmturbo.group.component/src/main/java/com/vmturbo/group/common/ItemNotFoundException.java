package com.vmturbo.group.common;

import java.util.List;

public class ItemNotFoundException extends Exception {
    private ItemNotFoundException(final String message) {
        super(message);
    }

    public static class GroupNotFoundException extends ItemNotFoundException {
        public GroupNotFoundException(final long id) {
            super("Group " + id + " not found.");
        }
    }

    public static class PolicyNotFoundException extends ItemNotFoundException {
        public PolicyNotFoundException(final long id) {
            super("Policy " + id + " not found.");
        }
    }

    public static class SettingPolicyNotFoundException extends ItemNotFoundException {
        public SettingPolicyNotFoundException(final long id) {
            super("Setting Policy " + id + " not found.");
        }
    }

    public static class SettingNotFoundException extends ItemNotFoundException {
        public SettingNotFoundException(final List<String> settingSpecName) {
            super("Setting " + settingSpecName + " not found.");
        }
    }

    /** This exception is thrown when schedule is not found in
     * {@link com.vmturbo.group.schedule.ScheduleStore}.
     * */
    public static class ScheduleNotFoundException extends ItemNotFoundException {
        /** Constructs an instance of {@link ScheduleNotFoundException}.
         * @param id Id of missing schedule
         * */
        public ScheduleNotFoundException(final long id) {
            super("Schedule " + id + " not found.");
        }

        /** Constructs an instance of {@link ScheduleNotFoundException}.
         * @param message Exception message
         */
        public ScheduleNotFoundException(String message) {
            super(message);
        }
    }
}
