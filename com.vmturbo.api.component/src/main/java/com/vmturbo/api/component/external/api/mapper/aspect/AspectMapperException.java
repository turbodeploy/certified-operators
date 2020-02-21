package com.vmturbo.api.component.external.api.mapper.aspect;

/**
 * General exception for Aspect Mappers.
 *
 */
public class AspectMapperException extends Exception {
    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new exception with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause the cause
     */
    public AspectMapperException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param message the detail message
     */
    public AspectMapperException(String message) {
        super(message);
    }
}
