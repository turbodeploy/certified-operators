package com.vmturbo.topology.processor.history.exceptions;

/**
 * Exception to be thrown when IO error happens during historical data processing.
 */
public class HistoryPersistenceException extends HistoryCalculationException {
    /**
     * Construct the exception instance.
     *
     * @param s message
     * @param throwable cause
     */
    public HistoryPersistenceException(String s, Throwable throwable) {
        super(s, throwable);
    }

    /**
     * Construct the exception instance.
     *
     * @param s message
     */
    public HistoryPersistenceException(String s) {
        super(s);
    }
}
