package com.vmturbo.topology.processor.entity;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.topology.processor.entity.EntityValidator.EntityValidationFailure;

/**
 * Exception thrown when discovered entities contain illegal values.
 */
public class EntitiesValidationException extends Exception {

    private final List<EntityValidationFailure> validationFailures;

    public EntitiesValidationException(
            @Nonnull final List<EntityValidationFailure> validationFailures) {
        super("Encountered errors with " + validationFailures.size() + " entities:\n" +
            validationFailures);
        this.validationFailures = Objects.requireNonNull(validationFailures);
    }
}
