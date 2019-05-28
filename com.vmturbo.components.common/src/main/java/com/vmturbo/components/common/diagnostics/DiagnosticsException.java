package com.vmturbo.components.common.diagnostics;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.commons.lang.exception.ExceptionUtils;

/**
 * An exception that can occur when capturing or restoring diagnostics.
 */
public class DiagnosticsException extends Exception {

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
