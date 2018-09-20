package com.vmturbo.topology.processor.targets;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

/**
 * Exception thrown during target registration if a target's account
 * values don't match the requirements provided by the associated
 * {@link com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo}
 * at probe registration time.
 */
public class InvalidTargetException extends TargetStoreException {
    private final List<String> errors;

    public InvalidTargetException(@Nonnull final List<String> errors) {
        super(Joiner.on("; ").join(errors));
        this.errors = ImmutableList.copyOf(Objects.requireNonNull(errors));
    }

    public InvalidTargetException(@Nonnull final String error) {
        super(error);
        this.errors = ImmutableList.copyOf(Collections.singletonList(error));
    }

    /**
     * Retrieve all the errors encountered when validating
     * the target.
     *
     * @return A list of string error messages.
     */
    public List<String> getErrors() {
        return errors;
    }
}
