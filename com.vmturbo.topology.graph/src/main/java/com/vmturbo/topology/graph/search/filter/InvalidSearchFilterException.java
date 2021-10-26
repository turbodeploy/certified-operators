package com.vmturbo.topology.graph.search.filter;

/**
 * {@link InvalidSearchFilterException} can occur when filter passed into topology graph has
 * unexpected structure.
 */
public class InvalidSearchFilterException extends Exception {
    private static final long serialVersionUID = -6583691007694944905L;

    /**
     * Creates {@link InvalidSearchFilterException} instance.
     *
     * @param message detail message that explains failure circumstances.
     */
    public InvalidSearchFilterException(String message) {
        super(message);
    }

    /**
     * Creates {@link InvalidSearchFilterException} instance.
     *
     * @param message detail message that explains failure circumstances.
     * @param cause exception that caused failure.
     */
    public InvalidSearchFilterException(String message, Throwable cause) {
        super(message, cause);
    }
}
