package com.vmturbo.topology.processor.conversions;

/**
 * Exception thrown when conversion of an entity fails.
 */
public class EntityConversionException extends RuntimeException {

    /**
     * Constructs a new EntityConversionException with the specified detail message.
     * The cause is not initialized, and may subsequently be initialized by a
     * call to {@link #initCause}.
     *
     * @param message the detail message. The detail message is saved for
     *                later retrieval by the {@link #getMessage()} method.
     */
    public EntityConversionException(final String message) {
        super(message);
    }

    /**
     * Constructs a new EntityConversionException with the specified detail message and
     * cause.  <p>Note that the detail message associated with
     * {@code cause} is <i>not</i> automatically incorporated in
     * this runtime exception's detail message.
     *
     * @param message the detail message (which is saved for later retrieval
     *                by the {@link #getMessage()} method).
     * @param cause   the cause (which is saved for later retrieval by the
     *                {@link #getCause()} method).  (A <tt>null</tt> value is
     *                permitted, and indicates that the cause is nonexistent or
     *                unknown.)
     */
    public EntityConversionException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
