package com.vmturbo.group.common;

/**
 * Exception thrown when invalid item actions is attempted.
 */
public class ItemActionException extends Exception {
    private ItemActionException(final String message) {
        super(message);
    }

    /**
     * Invalid schedule assignment exception.
     */
    public static class InvalidScheduleAssignmentException extends ItemActionException {
        /**
         * Construct an instance of {@link InvalidScheduleAssignmentException}.
         * @param message Exception message
         */
        public InvalidScheduleAssignmentException(final String message) {
            super(message);
        }
    }
}
