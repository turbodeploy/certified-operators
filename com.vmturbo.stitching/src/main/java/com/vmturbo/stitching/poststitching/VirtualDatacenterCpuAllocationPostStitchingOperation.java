package com.vmturbo.stitching.poststitching;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Post-stitching operation for the purpose of setting CPU Allocation commodity capacities for
 * Virtual Datacenters if not already set.
 *
 * If the Virtual Datacenter has exactly one provider that is also a Virtual Datacenter, the
 * provider's CPU Allocation capacity is used. Otherwise, the capacity is the sum
 * of the CPU Allocation capacities of all Physical Machine providers.
 */
public class VirtualDatacenterCpuAllocationPostStitchingOperation implements PostStitchingOperation {

    private static final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(@Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.entityTypeScope(EntityType.VIRTUAL_DATACENTER);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity>
    performOperation(@Nonnull final Stream<TopologyEntity> entities,
                     @Nonnull final EntitySettingsCollection settingsCollection,
                     @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        entities.filter(entity -> !getCommoditiesWithoutCapacity(entity).isEmpty()).forEach(entity -> {
            final TopologyEntity nearestTopCapacityAwareVdc =
                            getNearestTopCapacityAwareVdc(entity);
            if (nearestTopCapacityAwareVdc == null) {
                populateCapacityFromProviders(entity, resultBuilder, entity.getProviders());
                return;
            }
            final boolean providerHasCapacity =
                            getCommoditiesWithoutCapacity(nearestTopCapacityAwareVdc).isEmpty();
            if (providerHasCapacity) {
                handleConsumerVirtualDatacenter(entity, nearestTopCapacityAwareVdc,
                                resultBuilder);
                return;
            }
            final double capacity = populateCapacityFromProviders(nearestTopCapacityAwareVdc,
                            resultBuilder, nearestTopCapacityAwareVdc.getProviders());
            if (entity.getOid() == nearestTopCapacityAwareVdc.getOid() || capacity <= 0) {
                return;
            }
            resultBuilder.queueUpdateEntityAlone(entity,
                            entityForUpdate -> getCommoditiesWithoutCapacity(entityForUpdate)
                                            .forEach(commodity -> {
                                                logger.debug("Updating CPU Allocation capacity for Virtual Datacenter {} to {}",
                                                                entityForUpdate.getOid(),
                                                                capacity);
                                                commodity.setCapacity(capacity);
                                            }));
        });
        return resultBuilder.build();
    }

    @Nullable
    private static TopologyEntity getNearestTopCapacityAwareVdc(@Nonnull TopologyEntity entity) {
        final Collection<TopologyEntity> parentVdcs = entity.getProviders().stream()
                        .filter(p -> p.getEntityType() == EntityType.VIRTUAL_DATACENTER_VALUE)
                        .collect(Collectors.toSet());
        if (parentVdcs.isEmpty()) {
            return entity;
        }
        final Optional<TopologyEntity> anyVdcWithCapacity =
                        parentVdcs.stream().filter(p -> getCommoditiesWithoutCapacity(p).isEmpty())
                                        .findAny();
        return anyVdcWithCapacity.orElseGet(() -> parentVdcs.stream()
                        .map(p -> getNearestTopCapacityAwareVdc(p)).findAny().orElse(null));
    }

    /**
     * Get all the commodities that an entity is selling that need updating (commodity is
     * of type CPU Allocation and has no capacity, which sometimes presents as capacity == 0)
     *
     * @param entity the entity to check
     * @return a list of all commodities that need updates (may be empty)
     */
    @Nonnull
    private static Collection<CommoditySoldDTO.Builder> getCommoditiesWithoutCapacity(
                    @Nonnull final TopologyEntity entity) {
        return entity.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList().stream()
            .filter(commodity ->
                commodity.getCommodityType().getType() == CommodityType.CPU_ALLOCATION_VALUE &&
                    (!commodity.hasCapacity() || commodity.getCapacity() <= 0))
            .collect(Collectors.toSet());
    }

    /**
     * Determine and queue the necessary changes, if any, for a Virtual Datacenter entity with a
     * "consumer" role.
     *
     * @param entity The virtual datacenter entity to be changed if necessary
     * @param provider The virtual datacenter that the CPU allocation capacity should be drawn from
     * @param resultBuilder The resultBuilder with which to queue necessary changes
     */
    private static void handleConsumerVirtualDatacenter(@Nonnull final TopologyEntity entity,
                    @Nonnull final TopologyEntity provider,
                    @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        final Optional<CommoditySoldDTO> providerCpuAllocCommodity = findProviderCommodity(provider);

        providerCpuAllocCommodity.ifPresent(providerCommodity -> {
            final double providerCapacity = providerCommodity.getCapacity();
            resultBuilder.queueUpdateEntityAlone(entity, entityForUpdate ->
                getCommoditiesWithoutCapacity(entityForUpdate).forEach(commodity -> {
                    logger.debug("Setting CPU Allocation capacity of Virtual Datacenter {} to {} " +
                        "from host Virtual Datacenter {}", entityForUpdate.getOid(), providerCapacity,
                        provider.getOid());
                    commodity.setCapacity(providerCapacity);
                })
            );
        });
    }

    /**
     * Retrieves a CommoditySoldDTO of type CPU Allocation with capacity greater than zero if the
     * entity has one.
     *
     * @param entity The entity to search for the applicable commodity
     * @return An optional of the commodity if the entity has it and empty otherwise
     */
    @Nonnull
    private static Optional<CommoditySoldDTO> findProviderCommodity(
                    @Nonnull final TopologyEntity entity) {
        final Collection<CommoditySoldDTO> soldCommodities =
                        entity.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream()
                                        .filter(commodity -> commodity.getCommodityType().getType()
                                                        == CommodityType.CPU_ALLOCATION_VALUE
                                                        && commodity.hasCapacity()
                                                        && commodity.getCapacity() > 0)
                                        .collect(Collectors.toSet());
        final int soldCommoditiesNumber = soldCommodities.size();
        if (soldCommoditiesNumber > 1) {
            logger.error("Multiple CPU Allocation commodities sold by entity {}", entity.getOid());
            return Optional.empty();
        }
        return soldCommodities.stream().findAny();
    }

    /**
     * Calculates sum of the relevant commodity capacities bought from all specified providers.
     *
     * @param entity The virtual datacenter entity to be changed if necessary
     * @param resultBuilder The resultBuilder with which to queue necessary changes
     * @param providers collection of providers which capacity might be considered
     *                 as part of VDC capacity.
     * @return capacity calculated for entity from specified providers.
     */
    private static double populateCapacityFromProviders(@Nonnull final TopologyEntity entity,
                    @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder,
                    @Nonnull Collection<TopologyEntity> providers) {
        final Collection<String> relevantKeys =
                        entity.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                                        .stream()
                                        .flatMap(cbfp -> cbfp.getCommodityBoughtList().stream()
                                                        .filter(bought -> bought.getCommodityType()
                                                                        .getType()
                                                                        == CommodityType.CPU_ALLOCATION_VALUE)
                                                        .map(bought -> bought.getCommodityType()
                                                                        .getKey()))
                                        .collect(Collectors.toSet());
        final double capacity = calculateCapacity(providers, relevantKeys);
        if (capacity <= 0) {
            return capacity;
        }
        resultBuilder.queueUpdateEntityAlone(entity, entityForUpdate ->
            getCommoditiesWithoutCapacity(entityForUpdate).forEach(commodity -> {
                logger.debug("Updating CPU Allocation capacity for Virtual Datacenter {} to {}",
                    entityForUpdate.getOid(), capacity);
                commodity.setCapacity(capacity);
            })
        );
        return capacity;
    }


    /**
     * Determine the total CPU Allocation capacity of a provider Virtual Datacenter using the
     * capacities of all of its Physical Machine providers.
     *
     * This is in emulation of the calculation in legacy, where certain assumptions are made.
     *  - all relevant commodities bought by the VDC are iterated through, regardless of
     *    whether any of them are duplicates.
     *  - for each of the VDC's bought commodities, exactly 0 or 1 commodity sold with a
     *    matching key is retrieved from each provider. Duplicates are discarded.
     *
     * These assumptions are enforced in this calculation.
     *
     * @param providers the providers whose relevant commodities' capacities will be used in the
     *                  calculation
     * @param relevantKeys the keys of all relevant commodities bought by the VDC, to be
     *                     matched with commodities sold by PM providers.
     * @return the total capacity of the VDC provider
     */
    private static double calculateCapacity(@Nonnull Collection<TopologyEntity> providers,
                    @Nonnull Collection<String> relevantKeys) {
        return providers.stream().filter(provider -> provider.getEntityType()
                        == EntityType.PHYSICAL_MACHINE_VALUE)
                        .flatMap(VirtualDatacenterCpuAllocationPostStitchingOperation::filterProviderCommodities)
                        .filter(commodity -> relevantKeys
                                        .contains(commodity.getCommodityType().getKey()))
                        .map(CommoditySoldDTO::getCapacity)
                        .reduce((double)0, Double::sum);
    }

    /**
     * Filter the CPU Allocation commodities of a provider entity to remove those with duplicate keys.
     * @param provider the TopologyEntity whose commodities sold are being filtered
     * @return a stream of the provider's CPU Allocation commodities with unique keys
     */
    @Nonnull
    private static Stream<CommoditySoldDTO> filterProviderCommodities(
                    @Nonnull final TopologyEntity provider) {
        final Collection<String> uniqueKeys = new HashSet<>();
        return provider.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream()
            .filter(commodity -> commodity.getCommodityType().getType() == CommodityType.CPU_ALLOCATION_VALUE)
            .filter(commodity -> uniqueKeys.add(commodity.getCommodityType().getKey()));
    }


}
