package com.vmturbo.topology.processor.api;

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
}
