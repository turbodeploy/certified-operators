package com.vmturbo.repository.topology;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;

/**
 * Filter used for global supply chain queries.
 */
public class GlobalSupplyChainFilter {

    private final Set<Integer> ignoredEntityTypes;

    private final Set<Long> allowedOids;

    private final Optional<EnvironmentType> environmentType;

    public GlobalSupplyChainFilter(@Nonnull Set<Integer> ignoredEntityTypes,
                                   @Nonnull Set<Long> allowedOids,
                                   Optional<EnvironmentType> environmentType) {

        this.ignoredEntityTypes = Objects.requireNonNull(ignoredEntityTypes);
        this.allowedOids = Objects.requireNonNull(allowedOids);
        this.environmentType = environmentType;
    }

    public Set<Integer> getIgnoredEntityTypes() {
        return ignoredEntityTypes;
    }

    public Set<Long> getAllowedOids() {
        return allowedOids;
    }

    public Optional<EnvironmentType> getEnvironmentType() {
        return environmentType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ignoredEntityTypes, allowedOids, environmentType);
    }
}
