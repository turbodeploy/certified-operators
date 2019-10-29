package com.vmturbo.group.common;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;

public class ImmutableUpdateException extends Exception {
    private ImmutableUpdateException(final String message) {
        super(message);
    }

    /**
     * Exception to be thrown when an attempt has been made to change (add, remove, update) the
     * group that is not designed to be changed manually.
     */
    public static class ImmutableGroupUpdateException extends ImmutableUpdateException {
        public ImmutableGroupUpdateException(final Group group) {
            super("Attempt to modify immutable discovered group " + GroupProtoUtil.getGroupName(group));
        }

        /**
         * Constructs the exception.
         *
         * @param groupId group that attempt to modify was made
         * @param groupName group name
         */
        public ImmutableGroupUpdateException(final long groupId, final String groupName) {
            super("Attempt to modify immutable discovered group " + groupId + " display name " +
                    groupName);
        }

        /**
         * Constructs the exception with the free-form message.
         *
         * @param message message
         */
        public ImmutableGroupUpdateException(String message) {
            super(message);
        }
    }

    public static class ImmutablePolicyUpdateException extends ImmutableUpdateException {
        public ImmutablePolicyUpdateException(final String policyName) {
            super("Attempt to modify immutable discovered policy " + policyName);
        }
    }

    public static class ImmutableSettingPolicyUpdateException extends ImmutableUpdateException {
        public ImmutableSettingPolicyUpdateException(final String message) {
            super(message);
        }
    }

}
