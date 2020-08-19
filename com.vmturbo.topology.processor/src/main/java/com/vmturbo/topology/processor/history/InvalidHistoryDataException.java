package com.vmturbo.topology.processor.history;

/**
 * Exception to throw when the loaded data are malformed.
 */
public class InvalidHistoryDataException extends HistoryCalculationException {
    /**
     * Construct the exception instance.
     *
     * @param s message
     * @param throwable cause
     */
    public InvalidHistoryDataException(String s, Throwable throwable) {
        super(s, throwable);
    }

    /**
     * Construct the exception instance.
     *
     * @param s message
     */
    public InvalidHistoryDataException(String s) {
        super(s);
    }
}
