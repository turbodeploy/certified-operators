package com.vmturbo.topology.processor.topology;

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
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * A factory class which provides methods to clone entities when adding workloads. This class is
 * package-private and only used to clone entities in current package in {@link TopologyEditor}.
 */
class TopologyEntityCloneFactory {

    /**
     * Create a clone of a topology entity, modifying some values, including oid, display name, and
     * unplacing the shopping lists.
     *
     * @param entity           Source topology entity.
     * @param identityProvider Used to generate an oid for the clone.
     * @param cloneCounter     Counter of the entity to be cloned to be used in the display name.
     * @param topology         The entities in the topology, arranged by ID.
     * @return the cloned entity,
     */
    TopologyEntityDTO.Builder clone(@Nonnull final TopologyEntityDTO.Builder entity,
                                    @Nonnull final IdentityProvider identityProvider,
                                    final int cloneCounter,
                                    @Nonnull final Map<Long, Builder> topology) {
        return internalClone(entity, identityProvider, cloneCounter, topology, new HashMap<>());
    }

    /**
     * Create clones of consumer entities from corresponding cloned provider entity and add them in
     * the topology. This is specifically used to clone consumer entities when cloning a provider
     * entity so that plan result will take consumer data into consideration. Cloned consumers are
     * not movable.
     *
     * <p>For example, when adding ContainerPods in plan, we clone the corresponding consumer
     * Containers to calculate CPU/memory overcommitments for ContainerPlatformCluster.
     *
     * @param clonedProviderBuilder Given cloned provider entity builder.
     * @param topology              The entities in the topology, arranged by ID.
     * @param entityOrigin          Given {@link Origin}.
     * @param identityProvider      Used to generate an oid for the clone.
     * @param origProviderId        Original provider ID of the cloned provider entity.
     * @param cloneCounter          Counter of the entity to be cloned to be used in the display name.
     */
    void cloneConsumersFromClonedProvider(@Nonnull final TopologyEntity.Builder clonedProviderBuilder,
                                          @Nonnull final Map<Long, TopologyEntity.Builder> topology,
                                          @Nonnull final IdentityProvider identityProvider,
                                          @Nonnull final Origin entityOrigin,
                                          final long origProviderId,
                                          final int cloneCounter) {
        Map<Long, Long> origToClonedProviderIdMap = new HashMap<>();
        origToClonedProviderIdMap.put(origProviderId, clonedProviderBuilder.getOid());
        // Clone corresponding consumers of the given added entity
        for (TopologyEntity consumer : topology.get(origProviderId).getConsumers()) {
            TopologyEntityDTO.Builder consumerEntityDTOBuilder = consumer.getTopologyEntityDtoBuilder();
            TopologyEntityDTO.Builder clonedConsumerDTOBuilder =
                cloneConsumerFromClonedProvider(consumerEntityDTOBuilder,
                    identityProvider, cloneCounter, origToClonedProviderIdMap).setOrigin(entityOrigin);
            // Set controllable and suspendable to false to avoid generating actions on cloned consumers.
            // Consider this as allocation model, where we clone a provider along with corresponding
            // consumer resources but we won't run further analysis on the cloned consumers.
            clonedConsumerDTOBuilder.getAnalysisSettingsBuilder()
                .setControllable(false)
                .setSuspendable(false);
            // Set providerId to the cloned consumers to make sure cloned provider won't be suspended.
            TopologyEntity.Builder clonedConsumerBuilder =
                TopologyEntity.newBuilder(clonedConsumerDTOBuilder)
                    .setClonedFromEntity(consumerEntityDTOBuilder)
                    .addProvider(clonedProviderBuilder);
            topology.put(clonedConsumerDTOBuilder.getOid(), clonedConsumerBuilder);
            clonedProviderBuilder.addConsumer(clonedConsumerBuilder);
        }
    }

    /**
     * Create a clone of a consumer entity from corresponding cloned provider entity. This is
     * specifically used to clone consumer entity when cloning a provider entity so that plan result
     * will take consumer data into consideration. Cloned consumers are not movable.
     *
     * <p>For example, when adding ContainerPods in plan, we clone the corresponding consumer
     * Containers to calculate CPU/memory overcommitments for ContainerPlatformCluster.
     *
     * @param entity                    Source topology entity.
     * @param identityProvider          Used to generate an oid for the clone.
     * @param cloneCounter              Counter of the entity to be cloned to be used in the display name.
     * @param origToClonedProviderIdMap Map of original provider ID to cloned provider ID.
     * @return The cloned TopologyEntityDTO.Builder.
     */
    private TopologyEntityDTO.Builder cloneConsumerFromClonedProvider(
        @Nonnull final TopologyEntityDTO.Builder entity,
        @Nonnull final IdentityProvider identityProvider,
        final int cloneCounter,
        @Nonnull final Map<Long, Long> origToClonedProviderIdMap) {
        return internalClone(entity, identityProvider, cloneCounter, new HashMap<>(), origToClonedProviderIdMap);
    }

    /**
     * Create a clone of a topology entity, modifying some values, including oid, display name, and
     * unplacing the shopping lists.
     *
     * @param entity                    Source topology entity.
     * @param identityProvider          Used to generate an oid for the clone.
     * @param cloneCounter              Counter of the entity to be cloned to be used in the display name.
     * @param topology                  The entities in the topology, arranged by ID.
     * @param origToClonedProviderIdMap Map of original provider ID to cloned provider ID.
     * @return the cloned entity,
     */
    private TopologyEntityDTO.Builder internalClone(@Nonnull final TopologyEntityDTO.Builder entity,
                                                    @Nonnull final IdentityProvider identityProvider,
                                                    final int cloneCounter,
                                                    @Nonnull final Map<Long, TopologyEntity.Builder> topology,
                                                    @Nonnull final Map<Long, Long> origToClonedProviderIdMap) {
        final TopologyEntityDTO.Builder cloneBuilder = entity.clone()
            .clearCommoditiesBoughtFromProviders();
        // unplace all commodities bought, so that the market creates a Placement action for them.
        Map<Long, Long> oldProvidersMap = Maps.newHashMap();
        long noProvider = 0;
        for (CommoditiesBoughtFromProvider bought : entity.getCommoditiesBoughtFromProvidersList()) {
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
            CommoditiesBoughtFromProvider.Builder clonedProvider =
                CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(providerId)
                    .setMovable(movable)
                    .setProviderEntityType(bought.getProviderEntityType());
            // In legacy opsmgr, during topology addition, all constraints are
            // implicitly ignored. We do the same thing here.
            // A Commodity has a constraint if it has a key in its CommodityType.
            bought.getCommodityBoughtList().forEach(commodityBought -> {
                if (!commodityBought.getCommodityType().hasKey()) {
                    clonedProvider.addCommodityBought(commodityBought);
                }
            });
            // Create the Comm bought grouping if it will have at least one commodity bought
            if (!clonedProvider.getCommodityBoughtList().isEmpty()) {
                cloneBuilder.addCommoditiesBoughtFromProviders(clonedProvider.build());
                oldProvidersMap.put(providerId, oldProvider);
            }
        }

        long cloneId = identityProvider.getCloneId(entity);
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
                    connectedEntity.getEntityBuilder().addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                            .setKey("CommodityInClone::" + commType + "::" + cloneId)
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
            .setDisplayName(entity.getDisplayName() + " - Clone #" + cloneCounter)
            .setOid(cloneId)
            .putAllEntityPropertyMap(entityProperties);
    }
}
