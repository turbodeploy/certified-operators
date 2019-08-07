package com.vmturbo.topology.processor.history;

/**
 * Exception for use in historical data calculations.
 */
public class HistoryCalculationException extends Exception {
    /**
     * Construct the exception instance.
     *
     * @param s message
     * @param throwable cause
     */
    public HistoryCalculationException(String s, Throwable throwable) {
        super(s, throwable);
    }

    /**
     * Construct the exception instance.
     *
     * @param s message
     */
    public HistoryCalculationException(String s) {
        super(s);
    }

}
