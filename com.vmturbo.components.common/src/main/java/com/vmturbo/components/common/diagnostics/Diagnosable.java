package com.vmturbo.components.common.diagnostics;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.commons.lang.exception.ExceptionUtils;

/**
 * The {@link Diagnosable} is an object that should be included into the diagnostics provided
 * by the component. Any object can implement this interface, and it's the responsibility of the
 * component author to actually inject all objects implementing this interface into the
 * object responsible for assembling the diags.
 * <p>
 * TODO (roman, Sept 7 2017): Right now most diagnostic collection happens outside of the objects
 * that own the data, so this interface isn't properly used. We should have the objects that
 * provide diags implement this interface, but this will require some refactoring.
 * <p>
 * TODO (roman, Sept 7 2017): The interface dealing with lists of strings is clumsy. It should
 * just be streams, and we should have a framework where a common diagnostics handler can
 * iterate through {@link Diagnosable}s and manage the streams given to each.
 */
public interface Diagnosable {
    /**
     * Save the diags as a list of strings {@link Diagnosable#restoreDiags(List<String>)}.
     *
     * @throws DiagnosticsException When an exception occurs during diagnostics collection.
     *
     * @return The diags as a list of strings. The strings should NOT have newline characters.
     */
    @Nonnull
    List<String> collectDiags() throws DiagnosticsException;

    /**
     * Restore the diags saved by {@link Diagnosable#collectDiags()}.
     *
     * @param collectedDiags The diags collected from a previous call to
     *      {@link Diagnosable#collectDiags()}. Must be in the same order.
     * @throws DiagnosticsException When an exception occurs during diagnostics restoration.
     */
    void restoreDiags(@Nonnull final List<String> collectedDiags) throws DiagnosticsException;

    /**
     * An exception that can occur when capturing or restoring diagnostics.
     */
    class DiagnosticsException extends Exception {

        /**
         * Descriptions of all the errors that occurred.
         */
        private final List<String> errors;

        public DiagnosticsException(@Nonnull final List<String> errors) {
            this.errors = errors;
        }

        public DiagnosticsException(@Nonnull final String error) {
            this.errors = Collections.singletonList(error);
        }

        public DiagnosticsException(@Nonnull final Throwable cause) {
            super(cause);
            this.errors = Collections.emptyList();
        }

        public List<String> getErrors() {
            if (hasErrors()) {
                return errors;
            } else {
                return Collections.singletonList(ExceptionUtils.getStackTrace(this));
            }
        }

        public boolean hasErrors() {
            return !errors.isEmpty();
        }
    }
}
