package com.vmturbo.group.common;

/**
 * Exceptions thrown during object deletion.
 */
public class ItemDeleteException extends Exception {
    private ItemDeleteException(final String message) {
        super(message);
    }

    /**
     * This exception is thrown when attempt is made to delete schedule which is in use.
     */
    public static class ScheduleInUseDeleteException extends ItemDeleteException {
        /**
         * Construct an instance of {@link ScheduleInUseDeleteException}.
         * @param message Exception message
         */
        public ScheduleInUseDeleteException(final String message) {
            super(message);
        }
    }
}
