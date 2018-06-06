package com.vmturbo.group.common;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;

public class ImmutableUpdateException extends Exception {
    private ImmutableUpdateException(final String message) {
        super(message);
    }

    public static class ImmutableGroupUpdateException extends ImmutableUpdateException {
        public ImmutableGroupUpdateException(final Group group) {
            super("Attempt to modify immutable discovered group " + GroupProtoUtil.getGroupName(group));
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
