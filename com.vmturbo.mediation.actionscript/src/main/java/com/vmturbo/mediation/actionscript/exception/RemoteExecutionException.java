package com.vmturbo.mediation.actionscript.exception;

/**
 * An exception that occurs while trying to execute code on a remote system.
 */
public class RemoteExecutionException extends Exception {

    /**
     * Constructs a new exception with the specified detail message and
     * cause.  <p>Note that the detail message associated with
     * {@code cause} is <i>not</i> automatically incorporated in
     * this exception's detail message.
     *
     * @param message the detail message (which is saved for later retrieval
     *                by the {@link #getMessage()} method).
     * @param cause   the cause (which is saved for later retrieval by the
     *                {@link #getCause()} method).  (A <tt>null</tt> value is
     *                permitted, and indicates that the cause is nonexistent or
     *                unknown.)
     */
    public RemoteExecutionException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
