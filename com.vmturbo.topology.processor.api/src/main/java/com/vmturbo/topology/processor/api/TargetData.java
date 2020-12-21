package com.vmturbo.topology.processor.api;

import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

/**
 * Represents changeable target data.
 */
public interface TargetData {

    /**
     * Account values configuration for the specific target.
     *
     * @return account values
     */
    @Nonnull
    Set<AccountValue> getAccountData();

    /**
     * Returns the communication channel of the target.
     *
     * @return the communication channel of the target or Optional.empty if there is no channel
     * assigned
     */
    @Nonnull
    Optional<String> getCommunicationBindingChannel();
}
