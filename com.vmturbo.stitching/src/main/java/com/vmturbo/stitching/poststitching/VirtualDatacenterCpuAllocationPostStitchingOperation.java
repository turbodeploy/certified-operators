package com.vmturbo.stitching.poststitching;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

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
        entities.filter(entity -> !getFillableCommodities(entity).isEmpty()).forEach(entity -> {
            try {
                final Optional<TopologyEntity> virtualDatacenterProvider =
                    findVdcProvider(entity.getProviders(), entity.getOid());
                if (virtualDatacenterProvider.isPresent()) {
                    handleConsumerVirtualDatacenter(entity, virtualDatacenterProvider.get(), resultBuilder);
                } else {
                    handleProviderVirtualDatacenter(entity, resultBuilder);
                }
            } catch (IllegalStateException e) {
                logger.error(e.getMessage());
            }
        });
        return resultBuilder.build();
    }

    /**
     * Get all the commodities that an entity is selling that need updating (commodity is
     * of type CPU Allocation and has no capacity, which sometimes presents as capacity == 0)
     *
     * @param entity the entity to check
     * @return a list of all commodities that need updates (may be empty)
     */
    private List<CommoditySoldDTO.Builder> getFillableCommodities(@Nonnull final TopologyEntity entity) {
        return entity.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList().stream()
            .filter(commodity ->
                commodity.getCommodityType().getType() == CommodityType.CPU_ALLOCATION_VALUE &&
                    (!commodity.hasCapacity() || commodity.getCapacity() == 0))
            .collect(Collectors.toList());
    }

    /**
     * Determines whether a Virtual Datacenter has a provider Virtual Datacenter serving as host.
     * If an entity has exactly one Virtual Datacenter provider, it has a consumer role. Otherwise
     * it has a provider role. Note: there cannot be more than one Virtual Datacenter provider.
     *
     * @param providers the entities to filter
     * @param oid oid of the original Virtual Datacenter (for logging only)
     * @return an Optional of the Virtual Datacenter provider, or empty if there is none
     * @throws IllegalStateException if there are multiple Virtual Datacenter providers
     */
    private Optional<TopologyEntity> findVdcProvider(@Nonnull final List<TopologyEntity> providers,
                                                     final long oid) {
        return providers.stream()
            .filter(provider -> provider.getEntityType() == EntityType.VIRTUAL_DATACENTER_VALUE)
            .reduce((expected, unexpected) -> {
                throw new IllegalStateException("Found multiple Virtual Datacenter hosts for " +
                    "VirtualDatacenter " + oid + ". No change made to CPU Allocation capacity.");
            });
    }

    /**
     * Determine and queue the necessary changes, if any, for a Virtual Datacenter entity with a
     * "consumer" role.
     *
     * @param entity The virtual datacenter entity to be changed if necessary
     * @param provider The virtual datacenter that the CPU allocation capacity should be drawn from
     * @param resultBuilder The resultBuilder with which to queue necessary changes
     */
    private void handleConsumerVirtualDatacenter(@Nonnull final TopologyEntity entity,
                                                 @Nonnull final TopologyEntity provider,
                                                 @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        final Optional<CommoditySoldDTO> providerCpuAllocCommodity = findProviderCommodity(provider);

        providerCpuAllocCommodity.ifPresent(providerCommodity -> {
            final double providerCapacity = providerCommodity.getCapacity();
            resultBuilder.queueUpdateEntityAlone(entity, entityForUpdate ->
                getFillableCommodities(entityForUpdate).forEach(commodity -> {
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
     * @throws IllegalStateException if more than one qualifying commodity is found
     */
    private Optional<CommoditySoldDTO> findProviderCommodity(@Nonnull final TopologyEntity entity) {
        return entity.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream()
            .filter(commodity ->
                commodity.getCommodityType().getType() == CommodityType.CPU_ALLOCATION_VALUE &&
                    commodity.hasCapacity() && commodity.getCapacity() > 0)
            .reduce((expectedCommodity, unexpectedCommodity) -> {
                throw new IllegalStateException("Multiple CPU Allocation commodities sold by entity " +
                    entity.getOid());
            });
    }

    /**
     * Determine and queue the necessary changes, if any, for a Virtual Datacenter entity with a
     * "provider" role.
     * @param entity The virtual datacenter entity to be changed if necessary
     * @param resultBuilder The resultBuilder with which to queue necessary changes
     */
    private void handleProviderVirtualDatacenter(@Nonnull final TopologyEntity entity,
                                                 @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        Multiset<String> relevantKeys =
            entity.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().stream()
                .flatMap(commodityAndProvider ->
                    commodityAndProvider.getCommodityBoughtList().stream())
                .filter(commodity ->
                    commodity.getCommodityType().getType() == CommodityType.CPU_ALLOCATION_VALUE)
                .map(commodity -> commodity.getCommodityType().getKey())
                .collect(Collector.of(HashMultiset::create, Multiset::add, (set1, set2) -> {
                    set1.addAll(set2);
                    return set1;
                }));

        final double capacity = calculateCapacity(entity.getProviders(), relevantKeys);

        resultBuilder.queueUpdateEntityAlone(entity, entityForUpdate ->
            getFillableCommodities(entityForUpdate).forEach(commodity -> {
                logger.debug("Updating CPU Allocation capacity for Virtual Datacenter {} to {}",
                    entityForUpdate.getOid(), capacity);
                commodity.setCapacity(capacity);
            })
        );
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
    private double calculateCapacity(@Nonnull final List<TopologyEntity> providers,
                                     final Multiset<String> relevantKeys) {
        return providers.stream()
            .filter(provider -> provider.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE)
            .flatMap(this::filterProviderCommodities)
            .map(commodity -> commodity.getCapacity() *
                relevantKeys.count(commodity.getCommodityType().getKey())
            ).reduce((double) 0, Double::sum);
    }

    /**
     * Filter the CPU Allocation commodities of a provider entity to remove those with duplicate keys.
     * @param provider the TopologyEntity whose commodities sold are being filtered
     * @return a stream of the provider's CPU Allocation commodities with unique keys
     */
    private Stream<CommoditySoldDTO> filterProviderCommodities(@Nonnull final TopologyEntity provider) {
        final Set<String> uniqueKeys = new HashSet<>();
        return provider.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream()
            .filter(commodity -> commodity.getCommodityType().getType() == CommodityType.CPU_ALLOCATION_VALUE)
            .filter(commodity -> uniqueKeys.add(commodity.getCommodityType().getKey()));
    }


}
