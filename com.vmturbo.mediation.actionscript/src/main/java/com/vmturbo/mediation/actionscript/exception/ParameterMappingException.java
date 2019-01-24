package com.vmturbo.mediation.actionscript.exception;

/**
 * Indicates that a problem has happened while mapping parameter values prior to script execution.
 */
public class ParameterMappingException extends RuntimeException {

    /**
     * Constructs a new parameter mapping exception with the specified detail message.
     * The cause is not initialized, and may subsequently be initialized by a
     * call to {@link #initCause}.
     *
     * @param message the detail message. The detail message is saved for
     *                later retrieval by the {@link #getMessage()} method.
     */
    public ParameterMappingException(final String message) {
        super(message);
    }

    /**
     * Constructs a new parameter mapping exception with the specified detail message and
     * cause.  <p>Note that the detail message associated with
     * {@code cause} is <i>not</i> automatically incorporated in
     * this parameter mapping exception's detail message.
     *
     * @param message the detail message (which is saved for later retrieval
     *                by the {@link #getMessage()} method).
     * @param cause   the cause (which is saved for later retrieval by the
     *                {@link #getCause()} method).  (A <tt>null</tt> value is
     *                permitted, and indicates that the cause is nonexistent or
     *                unknown.)
     */
    public ParameterMappingException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
