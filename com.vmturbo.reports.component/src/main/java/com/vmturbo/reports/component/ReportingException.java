package com.vmturbo.reports.component;

/**
 * Exception to be thrown if error occurred operating with reports.
 */
public class ReportingException extends Exception {

    /**
     * Constructs reporting exception.
     *
     * @param message exception message
     */
    public ReportingException(String message) {
        super(message);
    }

    /**
     * Constructs reporting exception, wrapping some another exception.
     *
     * @param message exception message
     * @param cause root cause exception
     */
    public ReportingException(String message, Throwable cause) {
        super(message, cause);
    }
}
