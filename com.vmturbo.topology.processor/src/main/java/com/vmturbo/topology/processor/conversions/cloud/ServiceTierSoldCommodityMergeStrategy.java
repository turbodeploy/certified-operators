package com.vmturbo.topology.processor.conversions.cloud;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import com.vmturbo.platform.common.builders.CommodityBuilderIdentifier;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.utilities.MergeEntities.MergeCommoditySoldStrategy;

/**
 * The ServiceTierSoldCommodityMergeStrategy is designed for merging sold commodities on cloud
 * entities into service tier entities (ComputeTier, StorageTier). For example: merging individual
 * VirtualMachine or PhysicalMachine (AZ) entities into ComputeTier entity; merging storages into
 * StorageTier entity.
 *
 * We do want to track the "used" and "capacity" values of the commodities, both will be the
 * higher of the two values (merged from vs. onto). We can not add "used" from each merged
 * entity since it may exceed the capacity. For example: for StorageAmount commodity, its used is
 * always "7.0E8" and capacity "1.0E9".
 *
 * We will also be filtering out access commodities such as: DSPM access commodities, as these are
 * modeled via non-commodity relationships now. (i.e. the "connectedTo" relationship between a VM
 * and AvailabilityZone)
 */
public class ServiceTierSoldCommodityMergeStrategy implements MergeCommoditySoldStrategy {

    private static List<CommodityType> excludedCommodityTypes = ImmutableList.of(
            CommodityType.DSPM_ACCESS,
            CommodityType.VMPM_ACCESS,
            CommodityType.DATASTORE,
            CommodityType.DATACENTER
    );

    private Set<CommodityBuilderIdentifier> allowedCommodityIdentifiers;

    public ServiceTierSoldCommodityMergeStrategy() {
    }

    /**
     * Constructor for ServiceTierSoldCommodityMergeStrategy. "allowedCommodities" is a list of
     * bought commodity DTOs from the entity which consumes this ComputeTier. Only commodities of
     * those types and keys can be merged to this CT.
     */
    public ServiceTierSoldCommodityMergeStrategy(List<CommodityDTO.Builder> allowedCommodities) {
        allowedCommodityIdentifiers = allowedCommodities.stream().map(commodity ->
                new CommodityBuilderIdentifier(commodity.getCommodityType(), commodity.getKey()))
                .collect(Collectors.toSet());
    }

    @Nonnull
    @Override
    public Optional<Builder> onDistinctCommodity(@Nonnull final Builder commodity, final Origin origin) {
        // if this is a DSPM access commodity on the "from" entity, don't keep it
        if (origin == Origin.FROM_ENTITY &&
                excludedCommodityTypes.contains(commodity.getCommodityType())) {
            return Optional.empty();
        }

        // if this commodity "type + key" on the "from" entity does not exist in bought commodity,
        // don't keep it
        CommodityBuilderIdentifier commIdentifier = new CommodityBuilderIdentifier(
                commodity.getCommodityType(), commodity.getKey());
        if (origin == Origin.FROM_ENTITY && allowedCommodityIdentifiers != null &&
                !allowedCommodityIdentifiers.contains(commIdentifier)) {
            return Optional.empty();
        }

        return Optional.of(commodity);
    }

    @Nonnull
    @Override
    public Optional<Builder> onOverlappingCommodity(@Nonnull final Builder fromCommodity, @Nonnull final Builder ontoCommodity) {
        // merge the used and capacity settings.
        ontoCommodity.setUsed(Math.max(ontoCommodity.getUsed(), fromCommodity.getUsed()));
        ontoCommodity.setCapacity(Math.max(ontoCommodity.getCapacity(), fromCommodity.getCapacity()));
        return Optional.of(ontoCommodity);
    }
}
