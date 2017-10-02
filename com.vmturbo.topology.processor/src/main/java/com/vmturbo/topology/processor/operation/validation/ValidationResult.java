package com.vmturbo.topology.processor.operation.validation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;

/**
 * The result of a validation on a target.
 */
public class ValidationResult {
    /**
     * The ID of the target the result applies to.
     */
    private final long targetId;

    /**
     * The list of errors encountered when validating
     * the target. A valid target may still have
     * validation errors.
     */
    private final Map<ErrorSeverity, List<ErrorDTO>> errors;

    public ValidationResult(final long targetId,
                            @Nonnull final ValidationResponse validationResponse) {
        this.targetId = targetId;
        final Map<ErrorSeverity, ImmutableList.Builder<ErrorDTO>> builders = new HashMap<>();
        for (final ErrorSeverity severity : ErrorSeverity.values()) {
            builders.put(severity, new ImmutableList.Builder<>());
        }
        validationResponse.getErrorDTOList().stream()
                .forEach(error -> builders.get(error.getSeverity()).add(error));

        final ImmutableMap.Builder<ErrorSeverity, List<ErrorDTO>> errBuilder =
                new ImmutableMap.Builder<>();
        builders.entrySet().stream()
                .forEach(entry -> errBuilder.put(entry.getKey(), entry.getValue().build()));
        errors = errBuilder.build();
    }

    public long getTargetId() {
        return targetId;
    }

    public boolean isSuccess() {
        return errors.get(ErrorSeverity.CRITICAL).isEmpty();
    }

    @Nonnull
    public Map<ErrorSeverity, List<ErrorDTO>> getErrors() {
        return errors;
    }
}
