package com.vmturbo.topology.processor.topology.clone;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;
import com.google.gson.Gson;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.PlanScenarioOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * A class for the topology entity clone function.
 */
public class DefaultEntityCloneEditor {

    TopologyInfo topologyInfo;
    IdentityProvider identityProvider;

    DefaultEntityCloneEditor(@Nonnull final TopologyInfo topologyInfo,
                             @Nonnull final IdentityProvider identityProvider) {
        this.topologyInfo = topologyInfo;
        this.identityProvider = identityProvider;
    }

    /**
     * The function to clone a topology entity.
     *
     * @param entityDTOBuilder the builder of the source topology entity
     * @param cloneCounter counter of the entity to be cloned for display
     * @param topology the entities in the topology by id
     * @return the builder of the cloned topology entity
     */
    public TopologyEntity.Builder clone(@Nonnull TopologyEntityDTO.Builder entityDTOBuilder,
                                        long cloneCounter,
                                        @Nonnull Map<Long, Builder> topology) {
        // Create the new entity being added, but set the plan origin so these added
        // entities aren't counted in plan "current" stats
        // entities added in this stage will heave a plan origin pointed to the context id of this topology
        final TopologyEntityDTO.Builder clonedEntityDTOBuilder =
                internalClone(entityDTOBuilder, cloneCounter, topology, new HashMap<>());
        final Origin entityOrigin = Origin.newBuilder().setPlanScenarioOrigin(
                PlanScenarioOrigin.newBuilder()
                        .setPlanId(topologyInfo.getTopologyContextId())
                        .setOriginalEntityId(entityDTOBuilder.getOid())).build();
        final TopologyEntity.Builder entityBuilder = TopologyEntity
                .newBuilder(clonedEntityDTOBuilder.setOrigin(entityOrigin))
                .setClonedFromEntity(entityDTOBuilder);
        topology.put(entityBuilder.getOid(), entityBuilder);
        return entityBuilder;
    }

    /**
     * Create a clone of a topology entity, modify some values, including oid, display name, and
     * remove the shopping lists.
     *
     * @param entityDTOBuilder          Source topology entity.
     * @param cloneCounter              Counter of the entity to be cloned to be used in the display name.
     * @param topology                  The entities in the topology, arranged by ID.
     * @param origToClonedProviderIdMap Map of original provider ID to cloned provider ID.
     * @return the cloned entity,
     */
     TopologyEntityDTO.Builder internalClone(
            @Nonnull final TopologyEntityDTO.Builder entityDTOBuilder,
            final long cloneCounter,
            @Nonnull final Map<Long, Builder> topology,
            @Nonnull final Map<Long, Long> origToClonedProviderIdMap) {
        final TopologyEntityDTO.Builder cloneBuilder = entityDTOBuilder.clone()
                .clearCommoditiesBoughtFromProviders();
        // unplace all commodities bought, so that the market creates a Placement action for them.
        Map<Long, Long> oldProvidersMap = Maps.newHashMap();
        long noProvider = 0;
        for (CommoditiesBoughtFromProvider bought : entityDTOBuilder.getCommoditiesBoughtFromProvidersList()) {
            long oldProvider = bought.getProviderId();
            // If oldProvider is found in origToClonedProviderIdMap, corresponding provider of given
            // entity is also cloned. Set cloned provider id to CommoditiesBoughtFromProvider and
            // movable to false to make sure cloned provider won't be suspended and cloned consumer
            // entity won't move out of the cloned provider.
            // A given entity could have multiple providers, if oldProvider ID is not found in
            // origToClonedProviderIdMap, then corresponding oldProvider is not cloned, so set
            // providerId as "noProvider" to cloned CommoditiesBoughtFromProvider in this case.
            Long providerId = origToClonedProviderIdMap.get(oldProvider);
            boolean movable = false;
            if (providerId == null) {
                providerId = --noProvider;
                movable = true;
            }
            CommoditiesBoughtFromProvider.Builder clonedBoughtFromProvider =
                    CommoditiesBoughtFromProvider.newBuilder()
                            .setProviderId(providerId)
                            .setMovable(movable)
                            .setProviderEntityType(bought.getProviderEntityType());
            // In legacy opsmgr, during topology addition, all constraints are
            // implicitly ignored. We do the same thing here.
            // A Commodity has a constraint if it has a key in its CommodityType.
            bought.getCommodityBoughtList().forEach(commodityBought -> {
                if (!commodityBought.getCommodityType().hasKey()) {
                    clonedBoughtFromProvider.addCommodityBought(commodityBought);
                }
            });
            // Create the Comm bought grouping if it will have at least one commodity bought
            if (!clonedBoughtFromProvider.getCommodityBoughtList().isEmpty()) {
                cloneBuilder.addCommoditiesBoughtFromProviders(clonedBoughtFromProvider.build());
                oldProvidersMap.put(providerId, oldProvider);
            }
        }

        long cloneId = identityProvider.getCloneId(entityDTOBuilder);
        cloneBuilder.getCommoditySoldListBuilderList().stream()
                // Do not set the utilization to 0. The usage of clone should exactly be like the original.
                .filter(commSold -> AnalysisUtil.DSPM_OR_DATASTORE.contains(commSold.getCommodityType().getType()))
                .forEach(bicliqueCommSold -> {
                    // Set commodity sold for storage/host in case of a DSPM/DATASTORE commodity.
                    // This will make sure we have an edge for biclique creation between newly cloned host
                    // to original storages or newly cloned storage to original hosts.
                    TopologyEntity.Builder connectedEntity = topology.get(bicliqueCommSold.getAccesses());
                    if (connectedEntity != null) {
                        int commType =
                                bicliqueCommSold.getCommodityType().getType() == CommodityType.DSPM_ACCESS_VALUE
                                        ? CommodityType.DATASTORE_VALUE : CommodityType.DSPM_ACCESS_VALUE;
                        connectedEntity.getEntityBuilder().addCommoditySoldList(
                                CommoditySoldDTO.newBuilder().setCommodityType(
                                                TopologyDTO.CommodityType.newBuilder()
                                                        .setKey("CommodityInClone::" + commType + "::"
                                                                        + cloneId)
                                                        .setType(commType))
                                        .setAccesses(cloneId)
                                        .build());
                    }
                });

        Map<String, String> entityProperties =
                Maps.newHashMap(cloneBuilder.getEntityPropertyMapMap());
        if (!oldProvidersMap.isEmpty()) {
            // TODO: OM-26631 - get rid of unstructured data and Gson
            entityProperties.put(TopologyDTOUtil.OLD_PROVIDERS, new Gson().toJson(oldProvidersMap));
        }
        return cloneBuilder
                .setDisplayName(entityDTOBuilder.getDisplayName() + " - Clone #" + cloneCounter)
                .setOid(cloneId)
                .putAllEntityPropertyMap(entityProperties);
    }
}
