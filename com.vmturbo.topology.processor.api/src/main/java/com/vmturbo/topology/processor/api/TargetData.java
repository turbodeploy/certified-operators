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
    String TARGET_ADDRESS = "address";
    String TARGET_NAME_OR_ADDRESS = "nameOrAddress";
    Set<String> TARGET_ADDRESS_KEYS = Sets.newHashSet(TARGET_ADDRESS, TARGET_NAME_OR_ADDRESS);

    /**
     * Account values configuration for the specific target.
     *
     * @return account values
     */
    @Nonnull
    Set<AccountValue> getAccountData();

    /**
     * Return the target display name, given its data, if one exists.
     *
     * @return the display name, if one exists.
     */
    @Nonnull
    default Optional<String> getDisplayName() {
        return getAccountData().stream()
            .filter(accountValue -> TARGET_ADDRESS_KEYS.contains(accountValue.getName()))
            .findAny()
            .map(AccountValue::getStringValue);
    }
}
