package com.vmturbo.topology.processor.targets;

import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Holds custom scoping operations for any probe types that support them.  The
 * {@link GroupScopeResolver} uses these operations to convert an account value to an OID from
 * which it then extracts the scope properties.
 */
public class CustomScopingOperationLibrary {

    private static final Map<SDKProbeType, CustomScopingOperation> operationMap =
        new ImmutableMap.Builder<SDKProbeType, CustomScopingOperation>()
            .put(SDKProbeType.AZURE, new BusinessAccountBySubscriptionIdCustomScopingOperation())
        .build();

    /**
     * Get a custom scoping operation for a given probe type or Optional.empty if none is defined.
     *
     * @param probeType {@link SDKProbeType} to get the operation for.
     * @return {@link Optional} {@link CustomScopingOperation} for this probe type if it exists and
     * Optional.empty otherwise.
     */
    public Optional<CustomScopingOperation> getCustomScopingOperation(SDKProbeType probeType) {
        return Optional.ofNullable(operationMap.get(probeType));
    }
}
