/*
 * (C) Turbonomic 2019.
 */

package com.vmturbo.topology.processor.group.settings.applicators;

import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * {@link VmInstanceStorePolicyApplicator} creates bought commodity for {@link
 * EntityType#VIRTUAL_MACHINE} which is residing on the template which has instance store disks.
 */
public class VmInstanceStorePolicyApplicator
                extends InstanceStorePolicyApplicator<CommodityBoughtDTO.Builder> {
    private final TopologyGraph<TopologyEntity> topologyGraph;

    /**
     * Creates {@link VmInstanceStorePolicyApplicator} instance.
     *
     * @param topologyGraph topology graph used to find related entities by theirs
     *                 oids.
     */
    public VmInstanceStorePolicyApplicator(@Nonnull TopologyGraph<TopologyEntity> topologyGraph) {
        super(EntityType.VIRTUAL_MACHINE, CommodityBoughtDTO::newBuilder,
                        CommodityBoughtDTO.Builder::setCommodityType,
                        (builder, number) -> builder.setUsed(number.doubleValue()),
                        Function.identity(), builder -> builder.getCommodityType().getType());
        this.topologyGraph = Objects.requireNonNull(topologyGraph);
    }

    @Override
    protected void populateInstanceStoreCommodities(@Nonnull Builder entity) {
        final CommoditiesBoughtFromProvider.Builder computeTierProvider =
                        getComputeTierProvider(entity, EntityType.COMPUTE_TIER);
        if (computeTierProvider == null) {
            // Normal use-case for on-prem VMs
            return;
        }
        topologyGraph.getEntity(computeTierProvider.getProviderId())
                        .map(TopologyEntity::getTopologyEntityDtoBuilder)
                        .filter(InstanceStorePolicyApplicator::hasComputeTierInfo)
                        .map(item -> item.getTypeSpecificInfo().getComputeTier())
                        .ifPresent(computeTierInfo -> addCommodities(
                                        CommoditiesBoughtFromProvider.Builder::getCommodityBoughtBuilderList,
                                        CommoditiesBoughtFromProvider.Builder::addCommodityBought,
                                        computeTierProvider, computeTierInfo));
    }

    private CommoditiesBoughtFromProvider.Builder getComputeTierProvider(@Nonnull Builder entity,
                    @Nonnull EntityType providerType) {
        final Collection<CommoditiesBoughtFromProvider.Builder> boughtFromComputeTiers =
                        entity.getCommoditiesBoughtFromProvidersBuilderList().stream()
                                        .filter(provider -> provider.getProviderEntityType()
                                                        == providerType.getNumber())
                                        .collect(Collectors.toSet());
        final int amountOfComputeTierProviders = boughtFromComputeTiers.size();
        if (amountOfComputeTierProviders == 0) {
            // Normal use-case for on-prem VMs
            getLogger().debug("There are no '{}' providers for '{}'", providerType,
                            entity.getOid());
            return null;
        }
        final CommoditiesBoughtFromProvider.Builder result =
                        boughtFromComputeTiers.iterator().next();
        if (amountOfComputeTierProviders > 1) {
            getLogger().warn(
                            "There are more than one '{}' provider for '{}', using first one '{}'.",
                            providerType, entity.getOid(), result.getProviderId());
        }
        return result;
    }

}
