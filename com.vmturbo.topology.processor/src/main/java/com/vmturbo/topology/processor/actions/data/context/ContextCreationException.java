package com.vmturbo.topology.processor.actions.data.context;

/**
 * Exception to be throw weh context could not be created. Also used in actions
 * when context throws exception performing any operations.
 */
public class ContextCreationException extends Exception {

    /**
     * Constructs a new ContextCreationException with the specified detail message.
     * The cause is not initialized, and may subsequently be initialized by a
     * call to {@link #initCause}.
     *
     * @param message the detail message. The detail message is saved for
     *                later retrieval by the {@link #getMessage()} method.
     */
    public ContextCreationException(final String message) {
        super(message);
    }

    /**
     * Constructs a new ContextCreationException with the specified detail message and
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
    public ContextCreationException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public static ContextCreationException noEntity(final String entityType, final long entityId) {
        return new ContextCreationException("Missing " + entityType +
                " entity (ID " + entityId + ")");
    }

    public static ContextCreationException noEntityTargetInfo(final String entityType,
                                                              final long entityId,
                                                              final long targetId) {
        return new ContextCreationException("Missing target info for " + entityType +
                " entity (ID " + entityId + "). This means that target " + targetId +
                " has not discovered this entity.");

    }
}
