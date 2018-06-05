package com.vmturbo.topology.processor.supplychain.errors;

import javax.annotation.Nonnull;

/**
 * Generic class for supply chain validation errors.
 */
public class SupplyChainValidationException extends Exception {
    private final String fullMessage;

    /**
     * Supply chain error.
     *
     * @param probeDisplay related probe, if any.
     * @param targetDisplay related target, if any.
     * @param entityDisplay related entity, if any.
     * @param message error message.
     */
    public SupplyChainValidationException(
          String probeDisplay, String targetDisplay, String entityDisplay, @Nonnull String message) {
        fullMessage =
            (probeDisplay == null ?  "" : ("Probe: " + probeDisplay + ". ")) +
            (targetDisplay == null ? "" : ("Target: " + targetDisplay + ". ")) +
            (entityDisplay == null ? "" : ("Entity: " + entityDisplay + ". ")) +
            message;
    }

    @Nonnull
    @Override
    public String toString() {
        return fullMessage;
    }
}
