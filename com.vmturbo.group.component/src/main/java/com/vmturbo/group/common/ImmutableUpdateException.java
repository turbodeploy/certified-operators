package com.vmturbo.group.common;

public class ImmutableUpdateException extends Exception {
    private ImmutableUpdateException(final String message) {
        super(message);
    }

    public static class ImmutablePolicyUpdateException extends ImmutableUpdateException {
        public ImmutablePolicyUpdateException(final String policyName) {
            super("Attempt to modify immutable discovered policy " + policyName);
        }
    }
}
