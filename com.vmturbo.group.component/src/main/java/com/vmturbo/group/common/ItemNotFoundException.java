package com.vmturbo.group.common;

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
}
