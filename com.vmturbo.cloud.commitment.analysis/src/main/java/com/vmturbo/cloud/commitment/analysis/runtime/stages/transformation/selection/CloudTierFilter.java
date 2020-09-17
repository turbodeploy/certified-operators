package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.cloud.commitment.analysis.demand.CloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandScope;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandScope.ComputeTierDemandScope;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * A filter for {@link CloudTierDemand}.
 */
public class CloudTierFilter {

    /**
     * A blocking {@link CloudTierFilter}, which will reject all {@link CloudTierDemand}.
     */
    public static final CloudTierFilter BLOCKING_FILTER =
            new CloudTierFilter(Collections.emptySet(), TypeSpecificFilter.PASS_THROUGH_FILTER) {
                @Override
                public boolean filter(@Nonnull final CloudTierDemand cloudTierDemand) {
                    return false;
                }
            };

    private final Set<Long> cloudTiers;

    private final TypeSpecificFilter typeSpecificFilter;

    private CloudTierFilter(@Nonnull Collection<Long> cloudTiers,
                            @Nonnull TypeSpecificFilter typeSpecificFilter) {
        this.cloudTiers = ImmutableSet.copyOf(cloudTiers);
        this.typeSpecificFilter = typeSpecificFilter;
    }

    /**
     * Filters {@code cloudTierDemand}, based on its cloud tier OID and type-specific attributes (e.g.
     * the platform and tenancy for compute tier demand).
     * @param cloudTierDemand The target cloud tier demand to filter.
     * @return True, if the {@code cloudTierDemand} passes the filter. False otherwise.
     */
    public boolean filter(@Nonnull CloudTierDemand cloudTierDemand) {

        Preconditions.checkNotNull(cloudTierDemand);

        return (cloudTiers.isEmpty() || cloudTiers.contains(cloudTierDemand.cloudTierOid()))
                && typeSpecificFilter.filter(cloudTierDemand);
    }

    /**
     * A factory class for creating {@link CloudTierFilter} instances.
     */
    public static class CloudTierFilterFactory {

        /**
         * Constructs a new {@link CloudTierFilter}, based on the provided {@code demandScope} configuration.
         * Any filter attribute (cloud tier OID, type-specific attributes) that are empty in the config
         * will be ignored as filter criteria.
         * @param demandScope The demanc scope configuration.
         * @return The newly created {@link CloudTierFilter} instance.
         */
        @Nonnull
        public CloudTierFilter newFilter(@Nonnull DemandScope demandScope) {

            Preconditions.checkNotNull(demandScope);

            final TypeSpecificFilter typeSpecificFilter;
            if (demandScope.hasComputeTierScope()) {
                typeSpecificFilter = ComputeTierFilter.fromDemandScope(demandScope.getComputeTierScope());
            } else {
                typeSpecificFilter = TypeSpecificFilter.PASS_THROUGH_FILTER;
            }

            return new CloudTierFilter(demandScope.getCloudTierOidList(), typeSpecificFilter);
        }
    }

    /**
     * An interface for type-specific cloud tier filters (e.g. a filter for compute tier attributes
     * like platform and tenancy).
     */
    private interface TypeSpecificFilter {

        TypeSpecificFilter PASS_THROUGH_FILTER = (cloudTierDemand) -> true;

        boolean filter(@Nonnull CloudTierDemand cloudTierDemand);
    }

    /**
     * A type-specific cloud filter for filtering compute tier demand.
     */
    private static final class ComputeTierFilter implements TypeSpecificFilter {

        private final Set<OSType> platforms;

        private final Set<Tenancy> tenancies;

        private ComputeTierFilter(@Nonnull Collection<OSType> platforms,
                                  @Nonnull Collection<Tenancy> tenancies) {

            this.platforms = ImmutableSet.copyOf(platforms);
            this.tenancies = ImmutableSet.copyOf(tenancies);
        }

        @Override
        public boolean filter(@Nonnull final CloudTierDemand cloudTierDemand) {
            if (cloudTierDemand instanceof ComputeTierDemand) {
                final ComputeTierDemand computeTierDemand = (ComputeTierDemand)cloudTierDemand;
                return (platforms.isEmpty() || platforms.contains(computeTierDemand.osType()))
                        && (tenancies.isEmpty() || tenancies.contains(computeTierDemand.tenancy()));

            } else {
                return false;
            }
        }

        public static ComputeTierFilter fromDemandScope(@Nonnull ComputeTierDemandScope demandScope) {
            return new ComputeTierFilter(demandScope.getPlatformList(), demandScope.getTenancyList());
        }
    }
}
