package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstancePurchaseConstraints;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.ReservedInstanceSpecMatcher.ReservedInstanceSpecData;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;

/**
 * A filter class responsible for filtering RIs based on the purchasing constraints of an RI buy analysis.
 * This class is also responsible for secondary filtering like choosing the RI spec corresponding to the
 * smallest instance size within a family.
 */
public class RISpecPurchaseFilter {

    private final Logger logger = LogManager.getLogger();

    private final CloudTopology<TopologyEntityDTO> cloudTopology;

    private final Map<String, ReservedInstancePurchaseConstraints> purchaseConstraintsByProvider;


    private RISpecPurchaseFilter(@Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                                 @Nonnull Map<String, ReservedInstancePurchaseConstraints> purchaseConstraintsByProvider) {

        this.cloudTopology = Objects.requireNonNull(cloudTopology);

        // normalize the provider name to upper case
        this.purchaseConstraintsByProvider = Objects.requireNonNull(purchaseConstraintsByProvider).entrySet()
                .stream()
                .collect(ImmutableMap.toImmutableMap(
                        e -> e.getKey().toUpperCase(),
                        Map.Entry::getValue));
    }

    /**
     * Resolves the purchasing spec for a grouping of RI specs. The assumption of this code is that the RI
     * specs within the grouping have the same coverage scope.
     * @param riSpecDataGrouping A group of RI specs, which have the same coverage scope (e.g. all t3
     *                           size flexible RI specs within us-east-1).
     * @return The RI spec to purchase, or null if none of the specs within the grouping are advisable.
     */
    @Nullable
    public ReservedInstanceSpecData resolvePurchasingSpec(@Nonnull Collection<ReservedInstanceSpecData> riSpecDataGrouping) {

        return riSpecDataGrouping.stream()
                .filter(riSpecData -> riSpecData.couponsPerInstance() > 0)
                .filter(riSpecData -> filterByPurchasingConstraints(riSpecData))
                // Sort the RI specs by smallest to largest and then by OID as a tie-breaker.
                // In selecting the first RI spec, we'll recommend the smallest instance type
                // within a family.
                .sorted(Comparator.comparing(ReservedInstanceSpecData::couponsPerInstance)
                        .thenComparing(ReservedInstanceSpecData::reservedInstanceSpecId))
                .findFirst()
                .orElse(null);
    }

    /**
     * Filter method used to short list RISpecs that match the corresponding purchasing constraints.
     *
     * @param riSpecData the RI Spec data for which we want to resolve the constraints.
     * @return true if no constraints found or if applicable to the RISpec data.
     */
    private boolean filterByPurchasingConstraints(@Nonnull ReservedInstanceSpecData riSpecData) {
        final long regionId = riSpecData.reservedInstanceSpec()
                .getReservedInstanceSpecInfo().getRegionId();
        final Optional<TopologyEntityDTO> provider = cloudTopology.getServiceProvider(regionId);
        boolean constraintApplicable = false;
        if (provider.isPresent()) {
            String providerName = provider.get().getDisplayName();
            ReservedInstancePurchaseConstraints constraints =
                    purchaseConstraintsByProvider.get(providerName.toUpperCase());
            constraintApplicable =  constraints != null
                    && isRISpecWithinPurchaseConstraints(constraints, riSpecData.reservedInstanceSpec());
        } else {
            logger.warn("Service Provider not found for region {}", regionId);
        }
        return constraintApplicable;
    }

    private boolean isRISpecWithinPurchaseConstraints(
            @Nonnull ReservedInstancePurchaseConstraints purchaseConstraints,
            @Nonnull ReservedInstanceSpec riSpec) {

        final ReservedInstanceType riSpecType = riSpec.getReservedInstanceSpecInfo().getType();

        return purchaseConstraints.getOfferingClass() == riSpecType.getOfferingClass()
                && purchaseConstraints.getPaymentOption() == riSpecType.getPaymentOption()
                && purchaseConstraints.getTermInYears() == riSpecType.getTermYears();
    }

    /**
     * A factory class for constructing {@link RISpecPurchaseFilter} instances.
     */
    public static class RISpecPurchaseFilterFactory {

        /**
         * Constructs a new {@link RISpecPurchaseFilter}.
         * @param cloudTopology The cloud topology, used to resolve the service provider and map to
         *                      purchasing constraints.
         * @param purchaseConstraintsByProvider The purchasing constraints by service provider name.
         * @return The newly constructed {@link RISpecPurchaseFilter} instance.
         */
        @Nonnull
        public RISpecPurchaseFilter newFilter(@Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                                              @Nonnull Map<String, ReservedInstancePurchaseConstraints> purchaseConstraintsByProvider) {
            return new RISpecPurchaseFilter(cloudTopology, purchaseConstraintsByProvider);
        }
    }
}
