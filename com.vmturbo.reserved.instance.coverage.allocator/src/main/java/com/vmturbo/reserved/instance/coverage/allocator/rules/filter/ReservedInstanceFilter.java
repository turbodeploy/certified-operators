package com.vmturbo.reserved.instance.coverage.allocator.rules.filter;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregate;
import com.vmturbo.cloud.common.commitment.aggregator.ReservedInstanceAggregate;
import com.vmturbo.cloud.common.commitment.aggregator.ReservedInstanceAggregateInfo;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentLocation;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentScope;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;

/**
 * An implementation of {@link CloudCommitmentFilter}, specific to accepting reserved instances. Any
 * other cloud commitment type will be rejected.
 */
public class ReservedInstanceFilter implements CloudCommitmentFilter {

    private final ReservedInstanceFilterConfig filterConfig;

    /**
     * Constructs a {@link ReservedInstanceFilter} instance.
     * @param filterConfig The filter configuration.
     */
    public ReservedInstanceFilter(@Nonnull ReservedInstanceFilterConfig filterConfig) {
        this.filterConfig = Objects.requireNonNull(filterConfig);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public boolean filter(final CloudCommitmentAggregate commitmentAggregate) {
        if (commitmentAggregate.isReservedInstance()) {
            final ReservedInstanceAggregate riAggregate = commitmentAggregate.asReservedInstanceAggregate();
            final ReservedInstanceAggregateInfo aggregateInfo = riAggregate.aggregateInfo();

            return filterScope(aggregateInfo)
                    && filterLocation(aggregateInfo)
                    && filterPlatformFlexibility(aggregateInfo)
                    && filterSizeFlexibility(aggregateInfo);
        } else {
            return false;
        }
    }

    private boolean filterScope(@Nonnull ReservedInstanceAggregateInfo aggregateInfo) {

        final CloudCommitmentScope riScope = aggregateInfo.scopeInfo().getShared()
                ? CloudCommitmentScope.BILLING_FAMILY
                : CloudCommitmentScope.ACCOUNT;
        return filterConfig.scopes().isEmpty() || filterConfig.scopes().contains(riScope);

    }

    private boolean filterPlatformFlexibility(@Nonnull ReservedInstanceAggregateInfo aggregateInfo) {
        return filterConfig.isPlatformFlexible()
                .map(platformFlexible ->
                        platformFlexible == aggregateInfo.platformInfo().isPlatformFlexible())
                .orElse(true);
    }

    private boolean filterSizeFlexibility(@Nonnull ReservedInstanceAggregateInfo aggregateInfo) {
        return filterConfig.isSizeFlexible()
                .map(sizeFlexible -> sizeFlexible == aggregateInfo.tierInfo().isSizeFlexible())
                .orElse(true);
    }

    private boolean filterLocation(@Nonnull ReservedInstanceAggregateInfo aggregateInfo) {

        final CloudCommitmentLocation riLocation = aggregateInfo.zoneOid().isPresent()
                ? CloudCommitmentLocation.AVAILABILITY_ZONE
                : CloudCommitmentLocation.REGION;

        return filterConfig.locations().isEmpty() || filterConfig.locations().contains(riLocation);
    }

    /**
     * A {@link CloudCommitmentFilterConfig}, specific to filtering reserved instances.
     */
    @HiddenImmutableImplementation
    @Immutable
    public interface ReservedInstanceFilterConfig extends CloudCommitmentFilterConfig {

        /**
         * {@inheritDoc}.
         */
        @Override
        @Derived
        default CloudCommitmentType type() {
            return CloudCommitmentType.RESERVED_INSTANCE;
        }

        /**
         * Indicates whether RIs should be filtered based on platform flexibility. If the {@link Optional}
         * is empty, RIs will not be filtered based on platform flexibility.
         * @return An optional boolean indicating whether to filter against platform flexibility of RIs.
         */
        @Nonnull
        Optional<Boolean> isPlatformFlexible();

        /**
         * Indicates whether RIs should be filtered based on size flexibility. If the {@link Optional}
         * is empty, RIs will not be filtered based on size flexibility.
         * @return An optional boolean indicating whether to filter against size flexibility of RIs.
         */
        @Nonnull
        Optional<Boolean> isSizeFlexible();

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed builder instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for {@link ReservedInstanceFilterConfig} instances.
         */
        class Builder extends ImmutableReservedInstanceFilterConfig.Builder {}
    }
}
