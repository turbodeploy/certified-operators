package com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository;

import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CommonCost;

/**
 * Repository interface for interacting with the reserved_instance_spec cost database table.
 */
public interface PlanReservedInstanceSpecStore {
    /**
     * Queries the reserved_instance_spec table for the reserved instance spec that meets the specified criteria.
     *
     * @param regionId      The OID of the region for which to retrieve the reserved instance spec
     * @param tierId        The OID of the compute tier for which to retrieve the reserved instance spec
     * @param offeringClass The reserved instance offering class for which to retrieve the reserved instance spec
     * @param paymentOption The reserved instance payment option for which to retrieve the reserved instance spec
     * @param term          The reserved instance term (years) for which to retrieve the reserved instance spec
     * @param tenancy       The reserved instance tenancy for which to retrieve the reserved instance spec
     * @param osType        The OS type of the reserved instance for which to retrieve the reserved instance spec
     * @return A list of all reserved instance specs that match this criteria
     */
    List<Cost.ReservedInstanceSpec> getReservedInstanceSpecs(@Nonnull Long regionId,
                                                             @Nonnull Long tierId,
                                                             CloudCostDTO.ReservedInstanceType.OfferingClass offeringClass,
                                                             CommonCost.PaymentOption paymentOption,
                                                             int term,
                                                             CloudCostDTO.Tenancy tenancy,
                                                             CloudCostDTO.OSType osType);

    /**
     * Queries the reserved_instance_spec table for the reserved instance specs for the specified region, tier, and term.
     * This method has been added to support Azure, which only defines reserved instances for a template in a region for
     * the specified term.
     *
     * @param regionId The OID of the region for which to retrieve the reserved instance spec
     * @param tierId   The OID of the compute tier for which to retrieve the reserved instance spec
     * @param term     The reserved instance term (years) for which to retrieve the reserved instance spec
     * @return A list of all reserved instance specs that match this criteria
     */
    List<Cost.ReservedInstanceSpec> getReservedInstanceSpecs(@Nonnull Long regionId,
                                                             @Nonnull Long tierId,
                                                             int term);

}
