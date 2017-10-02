package com.vmturbo.action.orchestrator.api.impl;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.client.ApiClientException;

/**
 * Action orchestrator processing exception.
 */
public class ActionOrchestratorException extends ApiClientException {

    public ActionOrchestratorException(@Nonnull final String message) {
        super(Objects.requireNonNull(message));
    }

    public ActionOrchestratorException(@Nonnull final String message,
                                       @Nonnull final Throwable cause) {
        super(Objects.requireNonNull(message), Objects.requireNonNull(cause));
    }
}
