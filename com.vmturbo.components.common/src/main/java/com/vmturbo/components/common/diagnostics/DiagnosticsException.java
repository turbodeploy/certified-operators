package com.vmturbo.components.common.diagnostics;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import org.apache.commons.lang.exception.ExceptionUtils;

/**
 * An exception that can occur when capturing or restoring diagnostics.
 * Accumulates a number of errors as the diagnostic dump proceeds. This
 * allows the diagnostic dump the option to try to proceed, capturing
 * other aspects of the dump if possible.
 */
public class DiagnosticsException extends Exception {

    /**
     * Descriptions of all the errors that occurred.
     */
    private final List<String> errors;

    /**
     * Create a new instance with the given list of error strings.
     *
     * @param errors the list of errors to start with
     */
    public DiagnosticsException(@Nonnull final List<String> errors) {
        super('[' + String.join(",", errors) + ']');
        this.errors = errors;
    }

    /**
     * Create a exception to hold an error encountered when dumping diagnostics.
     *
     * @param error the text of the error message to record
     */
    public DiagnosticsException(@Nonnull final String error) {
        this(Collections.singletonList(error));
    }

    /**
     * Register a single error along with the cause.
     *
     * @param error the description of the error condition
     * @param cause the exception that represents the error
     */
    public DiagnosticsException(@Nonnull final String error, @Nonnull final Throwable cause) {
        super(error, cause);
        this.errors = Collections.singletonList(error);
    }

    /**
     * Register an error described only by a Throwable. Initialize the List holder
     * for subsequent error messages.
     *
     * @param cause the exception that represents the error
     */
    public DiagnosticsException(@Nonnull final Throwable cause) {
        super(cause.getMessage(), cause);
        this.errors = Collections.emptyList();
    }

    /**
     * Fetch the list of error message strings, or a single error message string
     * generated representing the underlying Throwable.
     *
     * @return the list of error messages recorded tha occurred during diagnostics dumping
     */
    public List<String> getErrors() {
        if (hasErrors()) {
            return ImmutableList.copyOf(errors);
        } else {
            return Collections.singletonList(ExceptionUtils.getStackTrace(this));
        }
    }

    /**
     * Test whether any errors have been stored.
     *
     * @return 'true' if any error messages have been stored here
     */
    public boolean hasErrors() {
        return !errors.isEmpty();
    }
}
