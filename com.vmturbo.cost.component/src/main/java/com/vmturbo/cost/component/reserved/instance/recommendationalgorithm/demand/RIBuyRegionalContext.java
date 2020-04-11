package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand;

import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * A regional context representing demand grouping by (region, account grouping, RI spec to purchase).
 * The underlying demand of this context represents a single input to the kernel algorithm.
 */
@Value.Immutable
public interface RIBuyRegionalContext {

    /**
     * The region associated with this context. While the underlying {@link RIBuyDemandCluster} instances
     * may be associated with a zone or region, they will be grouped at the region level, given we
     * always make recommendations for regional RIs.
     *
     * @return The region topology entity for this context.
     */
    @Nonnull
    TopologyEntityDTO region();

    /**
     * The RI spec to purchase, if the analysis determines demand is suffiecient to make a recommendation.
     * This RI spec is determined through the RI spec matcher.
     *
     * @return The RI spec to purchase.
     */
    @Nonnull
    ReservedInstanceSpec riSpecToPurchase();

    /**
     * The compute tier referenced by the RI spec. This entity is made available mostly for debug
     * and display purposes.
     *
     * @return The compute tier topology entity.
     */
    @Nonnull
    TopologyEntityDTO computeTier();

    /**
     * The account grouping identifier for this context. The grouping may represent demand clusters
     * for a single standalone account or may represent demand grouped across an entire billing family.
     *
     * @return The {@link AccountGroupingIdentifier} of this context.
     */
    @Nonnull
    AccountGroupingIdentifier accountGroupingId();

    /**
     * The underlying demand clusters associated with this context. These will represent the unique
     * set of data collected in the demand table.
     *
     * @return The set of {@link RIBuyDemandCluster} instances of this context.
     */
    @Nonnull
    Set<RIBuyDemandCluster> demandClusters();

    /**
     * This is a tag to use in logging this regional context, representing the (region, account
     * grouping ID, and RI spec) of this context.
     * @return The tag of this context, to use for logging purposes.
     */
    @Nonnull
    String contextTag();

    /**
     * The analysis tag of this context, representing the RILT## tag that is used as a unique ID
     * of an analysis.
     * @return The analysis tag.
     */
    @Nonnull
    String analysisTag();


    /**
     * The region OID of this region context, derived from {@link #region()}.
     * @return The region OID.
     */
    @Value.Derived
    default long regionOid() {
        return region().getOid();
    }

    /**
     * The compute tier OID of this region context, derived from {@link #computeTier()}.
     * @return The compute tier OID.
     */
    @Value.Derived
    default long computeTierOid() {
        return computeTier().getOid();
    }


    /**
     * The coupons per RI spec instance, derived from the {@link #computeTier()}.
     * @return The coupons per RI spec instance.
     */
    @Value.Derived
    default int couponsPerRecommendedInstance() {
        return computeTier().getTypeSpecificInfo()
                .getComputeTier()
                .getNumCoupons();
    }
}
