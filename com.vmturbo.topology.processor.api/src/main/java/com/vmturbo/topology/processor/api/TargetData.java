package com.vmturbo.topology.processor.api;

import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

/**
 * Represents changeable target data.
 */
public interface TargetData {
    /**
     * The 'displayName' for a target is taken from either the 'address' or 'nameOrAddress' property
     */
    Set<String> TARGET_ADDRESS_KEYS = Sets.newHashSet("address", "nameOrAddress");

    /**
     * Account values configuration for the specific target.
     *
     * @return account values
     */
    Set<AccountValue> getAccountData();

    /**
     * Return the target display name, given its data, if one exists.
     *
     * @param targetData target data.
     * @return the display name, if one exists.
     */
    @Nonnull
    static Optional<String> getDisplayName(@Nonnull TargetData targetData) {
        return
            targetData.getAccountData().stream()
                .filter(accountValue -> TARGET_ADDRESS_KEYS.contains(accountValue.getName()))
                .findAny()
                .map(AccountValue::getStringValue);
    }
}
