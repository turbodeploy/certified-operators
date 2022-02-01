package com.vmturbo.topology.processor.topology.clone;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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
            if (shouldSkipProvider(bought)) {
                continue;
            }
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
            bought.getCommodityBoughtList().forEach(commodityBought -> {
                if (shouldCopyBoughtCommodity(commodityBought)) {
                    final TopologyDTO.CommodityType commodityType = commodityBought.getCommodityType();
                    if (shouldReplaceBoughtKey(commodityType, bought.getProviderEntityType())) {
                        final TopologyDTO.CommodityType newType = newCommodityTypeWithClonedKey(commodityType, cloneCounter);
                        final TopologyDTO.CommodityBoughtDTO clonedBought = commodityBought.toBuilder()
                                .setCommodityType(newType).build();
                        clonedBoughtFromProvider.addCommodityBought(clonedBought);
                    } else {
                        clonedBoughtFromProvider.addCommodityBought(commodityBought);
                    }
                }
            });
            // Create the Comm bought grouping if it will have at least one commodity bought
            if (!clonedBoughtFromProvider.getCommodityBoughtList().isEmpty()) {
                cloneBuilder.addCommoditiesBoughtFromProviders(clonedBoughtFromProvider.build());
                oldProvidersMap.put(providerId, oldProvider);
            }
        }

        long cloneId = identityProvider.getCloneId(entityDTOBuilder);
        for (final CommoditySoldDTO.Builder commSold : cloneBuilder.getCommoditySoldListBuilderList()) {
            if (AnalysisUtil.DSPM_OR_DATASTORE.contains(commSold.getCommodityType().getType())) {
                // Set commodity sold for storage/host in case of a DSPM/DATASTORE commodity.
                // This will make sure we have an edge for biclique creation between newly cloned host
                // to original storages or newly cloned storage to original hosts.
                TopologyEntity.Builder connectedEntity = topology.get(commSold.getAccesses());
                if (connectedEntity != null) {
                    int commType =
                            commSold.getCommodityType().getType() == CommodityType.DSPM_ACCESS_VALUE
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
            }

            final TopologyDTO.CommodityType commodityType = commSold.getCommodityType();
            if (shouldReplaceSoldKey(commodityType)) {
                commSold.setCommodityType(newCommodityTypeWithClonedKey(commodityType, cloneCounter));
            }
        }

        Map<String, String> entityProperties =
                Maps.newHashMap(cloneBuilder.getEntityPropertyMapMap());
        if (!oldProvidersMap.isEmpty()) {
            // TODO: OM-26631 - get rid of unstructured data and Gson
            entityProperties.put(TopologyDTOUtil.OLD_PROVIDERS, new Gson().toJson(oldProvidersMap));
        }
        return cloneBuilder
                .setDisplayName(entityDTOBuilder.getDisplayName() + cloneSuffix(cloneCounter))
                .setOid(cloneId)
                .putAllEntityPropertyMap(entityProperties);
    }

    /**
     * Whether to skip this provider and skip copying all the commodities from this provider.
     * Default is false, not to skip.
     *
     * @param boughtFromProvider the commodities bought from the provider
     * @return true if we should skip this provider, or otherwise false
     */
    protected boolean shouldSkipProvider(
            @Nonnull final CommoditiesBoughtFromProvider boughtFromProvider) {
        return false;
    }

    /**
     * Return true if we should copy the bought commodity.  By default, we will copy if it is not
     * a constraint, i.e., a commodity has key.  This default behavior comes from the legacy OpsMgr:
     * during topology addition, all constraints are implicitly ignored. We do the same thing here.
     *
     * <p>Subclasses such as {@link ContainerPodCloneEditor} could override this.
     *
     * @param commodityBought the commodity bought DTO
     * @return true if we should copy the bought commodity; otherwise, return false.
     */
    protected boolean shouldCopyBoughtCommodity(
            @Nonnull final TopologyDTO.CommodityBoughtDTO commodityBought) {
         return !Objects.requireNonNull(commodityBought).getCommodityType().hasKey();
    }

    /**
     * Whether to replace the key in the bought commodity with a distinct one.  Use cases include
     * the VMPM access commodity that a container pod sells to its containers to keep them together.
     * Default is false, not to replace.
     *
     * @param commodityType the bought commodity type
     * @param providerEntityType the provider entity type
     * @return true if we should replace the key, or otherwise false
     */
    protected boolean shouldReplaceBoughtKey(@Nonnull final TopologyDTO.CommodityType commodityType,
            final int providerEntityType) {
        return false;
    }

    /**
     * Whether to replace the key in the sold commodity with a distinct one.  Use cases include the
     * VMPM access commodity that a container pod sells to its containers to keep them together.
     * Default is false, not to replace.
     *
     * @param commodityType the sold commodity type
     * @return true if we should replace the key, or otherwise false
     */
    protected boolean shouldReplaceSoldKey(@Nonnull final TopologyDTO.CommodityType commodityType) {
        return false;
    }

    /**
     * Return a new {@link TopologyDTO.CommodityType} same as the input one except replacing the key
     * by adding a suffix indicating the clone counter.
     *
     * @param commodityType the original {@link TopologyDTO.CommodityType} which key to be replaced
     * @param cloneCounter the clone counter to be added to the key to make it distinct
     * @return the new {@link TopologyDTO.CommodityType} with the key replaced
     */
    private static TopologyDTO.CommodityType newCommodityTypeWithClonedKey(
            @Nonnull final TopologyDTO.CommodityType commodityType, long cloneCounter) {
        final String newKey = Objects.requireNonNull(commodityType).getKey() + cloneSuffix(cloneCounter);
        return commodityType.toBuilder().setKey(newKey).build();
    }

    /**
     * Abstract the way of generating a clone suffix.  Use cases include appending the suffix to
     * the display name and the commodity key of the clone to make it distinct.
     *
     * @param cloneCounter the clone counter to be part of the suffix to make it distinct
     * @return the constructed clone suffix
     */
    public static String cloneSuffix(long cloneCounter) {
        return " - Clone #" + cloneCounter;
    }
}
