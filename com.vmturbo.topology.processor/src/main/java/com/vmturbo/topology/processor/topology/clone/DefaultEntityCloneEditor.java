package com.vmturbo.topology.processor.topology.clone;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Maps;
import com.google.gson.Gson;

import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.OriginImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.OriginView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.PlanScenarioOriginImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityView;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.util.TopologyEditorUtil;

/**
 * A class for the topology entity clone function.
 */
public class DefaultEntityCloneEditor {

    /**
     * Enum that defines relations of related entities.
     */
    enum Relation {
        Provider,
        Consumer,
        Owned
    }

    /**
     * The function to clone a topology entity.
     *
     * @param entityImpl the builder of the source topology entity
     * @param topologyGraph the topology graph
     * @param cloneContext the clone context
     * @param cloneInfo the clone info related to this clone
     * @return the builder of the cloned topology entity
     */
    public TopologyEntity.Builder clone(@Nonnull final TopologyEntityImpl entityImpl,
                                        @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
                                        @Nonnull final CloneContext cloneContext,
                                        @Nonnull final CloneInfo cloneInfo) {
        final long cloneCounter = cloneInfo.getCloneCounter();
        if (cloneContext.isEntityCloned(cloneCounter, entityImpl.getOid())) {
            return cloneContext.getTopology().get(
                    cloneContext.getClonedEntityId(cloneCounter, entityImpl.getOid()));
        }
        // Create the new entity being added, but set the plan origin so these added
        // entities aren't counted in plan "current" stats
        // entities added in this stage will heave a plan origin pointed to the context id of this topology
        final TopologyEntityImpl clonedEntityImpl =
                internalClone(entityImpl, cloneContext, cloneInfo);
        final PlanScenarioOriginImpl planScenarioOrigin = new PlanScenarioOriginImpl()
                .setPlanId(cloneContext.getPlanId())
                .setOriginalEntityId(entityImpl.getOid());
        if (entityImpl.hasOrigin() && entityImpl.getOrigin().hasDiscoveryOrigin()) {
            planScenarioOrigin.addAllOriginalEntityDiscoveringTargetIds(
                    entityImpl.getOrigin().getDiscoveryOrigin().getDiscoveredTargetDataMap().keySet());
        }
        final OriginView entityOrigin = new OriginImpl().setPlanScenarioOrigin(planScenarioOrigin);
        final TopologyEntity.Builder entityBuilder = TopologyEntity
                .newBuilder(clonedEntityImpl.setOrigin(entityOrigin))
                .setClonedFromEntity(entityImpl);
        updateAnalysisSettings(entityBuilder, topologyGraph, cloneContext);
        cloneContext.getTopology().put(entityBuilder.getOid(), entityBuilder);
        cloneContext.cacheClonedEntityId(cloneCounter, entityImpl.getOid(), clonedEntityImpl.getOid());
        return entityBuilder;
    }

    /**
     * Create a clone of a topology entity, modify some values, including oid, display name, and
     * remove the shopping lists.
     *
     * @param entityImpl                Source topology entity.
     * @param cloneContext              The clone context
     * @return the cloned entity,
     */
    TopologyEntityImpl internalClone(@Nonnull final TopologyEntityImpl entityImpl,
                                     @Nonnull final CloneContext cloneContext,
                                     @Nonnull final CloneInfo cloneInfo) {
        final long cloneCounter = cloneInfo.getCloneCounter();
        final TopologyEntityImpl clonedEntityImpl =
                entityImpl.copy().clearCommoditiesBoughtFromProviders();
        // unplace all commodities bought, so that the market creates a Placement action for them.
        Map<Long, Long> oldProvidersMap = Maps.newHashMap();
        long noProvider = 0;
        for (CommoditiesBoughtFromProviderView bought : entityImpl.getCommoditiesBoughtFromProvidersList()) {
            if (shouldSkipProvider(bought, cloneContext)) {
                continue;
            }
            long origProviderId = bought.getProviderId();
            // If oldProvider is found in origToClonedProviderIdMap, corresponding provider of given
            // entity is also cloned. Set cloned provider id to CommoditiesBoughtFromProvider and
            // movable to false to make sure cloned provider won't be suspended and cloned consumer
            // entity won't move out of the cloned provider.
            // A given entity could have multiple providers, if oldProvider ID is not found in
            // origToClonedProviderIdMap, then corresponding oldProvider is not cloned, so set
            // providerId as "noProvider" to cloned CommoditiesBoughtFromProvider in this case.
            Long providerId = getProviderId(cloneContext, cloneInfo, origProviderId);
            boolean movable = false;
            if (providerId == null) {
                providerId = --noProvider;
                movable = true;
            }
            CommoditiesBoughtFromProviderImpl clonedBoughtFromProvider =
                    new CommoditiesBoughtFromProviderImpl()
                            .setProviderId(providerId)
                            .setMovable(movable)
                            .setProviderEntityType(bought.getProviderEntityType());
            bought.getCommodityBoughtList().forEach(commodityBought -> {
                if (shouldCopyBoughtCommodity(commodityBought, cloneContext, entityImpl)) {
                    final CommodityTypeView commodityType = commodityBought.getCommodityType();
                    if (shouldReplaceBoughtKey(commodityType, bought.getProviderEntityType())) {
                        final CommodityTypeView newType = getReplacedBoughtCommodity(
                                commodityType, cloneContext, cloneInfo);
                        final CommodityBoughtView clonedBought = commodityBought.copy()
                                .setCommodityType(newType);
                        clonedBoughtFromProvider.addCommodityBought(clonedBought);
                    } else {
                        clonedBoughtFromProvider.addCommodityBought(commodityBought);
                    }
                }
            });
            // Create the Comm bought grouping if it will have at least one commodity bought
            if (!clonedBoughtFromProvider.getCommodityBoughtList().isEmpty()) {
                clonedEntityImpl.addCommoditiesBoughtFromProviders(clonedBoughtFromProvider);
                oldProvidersMap.put(providerId, origProviderId);
            }
        }

        long cloneId = cloneContext.getIdProvider().getCloneId(entityImpl);
        for (final CommoditySoldImpl commSold : clonedEntityImpl.getCommoditySoldListImplList()) {
            if (AnalysisUtil.DSPM_OR_DATASTORE.contains(commSold.getCommodityType().getType())) {
                // Set commodity sold for storage/host in case of a DSPM/DATASTORE commodity.
                // This will make sure we have an edge for biclique creation between newly cloned host
                // to original storages or newly cloned storage to original hosts.
                TopologyEntity.Builder connectedEntity = cloneContext.getTopology().get(commSold.getAccesses());
                if (connectedEntity != null) {
                    int commType =
                            commSold.getCommodityType().getType() == CommodityType.DSPM_ACCESS_VALUE
                                    ? CommodityType.DATASTORE_VALUE : CommodityType.DSPM_ACCESS_VALUE;
                    connectedEntity.getTopologyEntityImpl().addCommoditySoldList(
                            new CommoditySoldImpl().setCommodityType(
                                            new CommodityTypeImpl()
                                                    .setKey("CommodityInClone::" + commType + "::"
                                                            + cloneId)
                                                    .setType(commType))
                                    .setAccesses(cloneId));
                }
            }

            final CommodityTypeView commodityType = commSold.getCommodityType();
            if (shouldReplaceSoldKey(commodityType)) {
                commSold.setCommodityType(newCommodityTypeWithClonedKey(commodityType, cloneCounter));
            }
            if (TopologyEditorUtil.isQuotaCommodity(commodityType.getType())) {
                // Remove any existing quota for cloned entity
                commSold.setCapacity(TopologyEditorUtil.MAX_QUOTA_CAPACITY);
            }
        }

        Map<String, String> entityProperties =
                Maps.newHashMap(clonedEntityImpl.getEntityPropertyMapMap());
        if (!oldProvidersMap.isEmpty()) {
            // TODO: OM-26631 - get rid of unstructured data and Gson
            entityProperties.put(TopologyDTOUtil.OLD_PROVIDERS, new Gson().toJson(oldProvidersMap));
        }
        return clonedEntityImpl
                .setDisplayName(getCloneDisplayName(entityImpl, cloneInfo))
                .setOid(cloneId)
                .putAllEntityPropertyMap(entityProperties);
    }

    /**
     * Whether to skip this provider and skip copying all the commodities from this provider.
     * Default is false, not to skip.
     *
     * @param boughtFromProvider the commodities bought from the provider
     * @param cloneContext the clone context
     * @return true if we should skip this provider, or otherwise false
     */
    protected boolean shouldSkipProvider(
            @Nonnull final CommoditiesBoughtFromProviderView boughtFromProvider,
            @Nonnull final CloneContext cloneContext) {
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
     * @param cloneContext the clone context
     * @param entity the entity to clone
     * @return true if we should copy the bought commodity; otherwise, return false.
     */
    protected boolean shouldCopyBoughtCommodity(
            @Nonnull final CommodityBoughtView commodityBought,
            @Nonnull final CloneContext cloneContext,
            @Nonnull final TopologyEntityView entity) {
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
    protected boolean shouldReplaceBoughtKey(@Nonnull final CommodityTypeView commodityType,
                                             final int providerEntityType) {
        return false;
    }

    /**
     * Get the replaced bought commodity.
     *
     * @param commodityType the commodity type
     * @param cloneContext the clone context
     * @param cloneInfo the clone info
     * @return the replaced bought commodity
     */
    protected CommodityTypeView getReplacedBoughtCommodity(@Nonnull final CommodityTypeView commodityType,
                                                           @Nonnull final CloneContext cloneContext,
                                                           @Nonnull final CloneInfo cloneInfo) {
        return newCommodityTypeWithClonedKey(commodityType, cloneInfo.getCloneCounter());
    }

    /**
     * Whether to replace the key in the sold commodity with a distinct one.  Use cases include the
     * VMPM access commodity that a container pod sells to its containers to keep them together.
     * Default is false, not to replace.
     *
     * @param commodityType the sold commodity type
     * @return true if we should replace the key, or otherwise false
     */
    protected boolean shouldReplaceSoldKey(@Nonnull final CommodityTypeView commodityType) {
        return false;
    }

    /**
     * Get the provider ID of the cloned entity.
     *
     * @param cloneContext the clone context
     * @param cloneInfo the clone change info
     * @param origProviderId the original provider ID
     * @return the provider ID of the cloned entity
     */
    @Nullable
    protected Long getProviderId(@Nonnull final CloneContext cloneContext,
                                 @Nonnull final CloneInfo cloneInfo,
                                 final long origProviderId) {
        return cloneContext.getClonedEntityId(cloneInfo.getCloneCounter(), origProviderId);
    }

    /**
     * Return a new {@link CommodityTypeView} same as the input one except replacing the key
     * by adding a suffix indicating the clone counter.
     *
     * @param commodityType the original {@link CommodityTypeView} which key to be replaced
     * @param cloneCounter the clone counter to be added to the key to make it distinct
     * @return the new {@link CommodityTypeView} with the key replaced
     */
    private static CommodityTypeView newCommodityTypeWithClonedKey(
            @Nonnull final CommodityTypeView commodityType, long cloneCounter) {
        final String newKey = Objects.requireNonNull(commodityType).getKey() + cloneSuffix(cloneCounter);
        return commodityType.copy().setKey(newKey);
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

    /**
     * Get the display name of the cloned entity.
     *
     * @param entityImpl the entity to clone
     * @param cloneInfo the clone change info
     * @return the display name of the cloned entity
     */
    protected String getCloneDisplayName(@Nonnull final TopologyEntityImpl entityImpl,
                                       @Nonnull final CloneInfo cloneInfo) {
        return cloneInfo.getSourceCluster()
                .map(TopologyEntity.Builder::getDisplayName)
                .map(clonedFrom -> String.format("%s - Clone from %s", entityImpl.getDisplayName(), clonedFrom))
                .orElse(entityImpl.getDisplayName() + cloneSuffix(cloneInfo.getCloneCounter()));
    }

    /**
     * Clone entities related to the given entity.
     *
     * @param origEntity the original entity
     * @param topologyGraph the topology graph
     * @param cloneContext the clone context
     * @param cloneInfo the clone change info
     * @param relation the relation to the original entity
     * @param entityType the entity type to clone
     * @return the collection of cloned topology entityies in the builder form
     */
    protected Collection<Builder> cloneRelatedEntities(@Nonnull final Builder origEntity,
                                        @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
                                        @Nonnull final CloneContext cloneContext,
                                        @Nonnull final CloneInfo cloneInfo,
                                        @Nonnull final Relation relation,
                                        final int entityType) {
        final DefaultEntityCloneEditor entityCloneEditor = EntityCloneEditorFactory.createEntityCloneFunction(entityType);
        final List<TopologyEntity> sourceEntities;
        switch (relation) {
            case Provider:
                sourceEntities = origEntity.getProviders();
                break;
            case Consumer:
                sourceEntities = origEntity.getConsumers();
                break;
            case Owned:
                sourceEntities = origEntity.getOwnedEntities();
                break;
            default:
                return Collections.emptyList();
        }
        return sourceEntities.stream()
                .map(TopologyEntity::getTopologyEntityImpl)
                .filter(entity -> entity.getEntityType() == entityType)
                .map(entity -> entityCloneEditor.clone(entity, topologyGraph,
                                                           cloneContext, cloneInfo))
                .collect(Collectors.toList());
    }

    /**
     * Replace connected entities for a given entity.
     *
     * @param entity the given entity
     * @param cloneContext the clone context
     * @param cloneInfo the clone change info
     * @param connectionType the connection type to replace
     */
    protected void replaceConnectedEntities(@Nonnull final TopologyEntity.Builder entity,
                                            @Nonnull final CloneContext cloneContext,
                                            @Nonnull final CloneInfo cloneInfo,
                                            final int connectionType) {
        entity.getTopologyEntityImpl().getConnectedEntityListImplList()
                .forEach(connectedEntityImpl -> {
                    if (connectedEntityImpl.getConnectionType().getNumber() == connectionType) {
                        Long clonedEntityId = cloneContext.getClonedEntityId(
                                cloneInfo.getCloneCounter(), connectedEntityImpl.getConnectedEntityId());
                        // When changing the number of replicas (pods and containers) of a
                        // WorkloadController as part of a Workload Migration Plan, we mutate the
                        // cloneCounter of each replica, but the workloadController and
                        // containerSpec entities are only cloned once. So we need to specify a
                        // cloneCounter of 0 to locate the cloned workloadController and cloned
                        // containerSpec to establish the connection.
                        if (clonedEntityId == null && cloneInfo.getCloneCounter() > 0) {
                            clonedEntityId = cloneContext.getClonedEntityId(
                                0, connectedEntityImpl.getConnectedEntityId());
                        }
                        if (clonedEntityId == null) {
                            return;
                        }
                        connectedEntityImpl.setConnectedEntityId(clonedEntityId);
                    }
                });
    }

    protected void updateAnalysisSettings(@Nonnull final TopologyEntity.Builder entity,
                                          @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
                                          @Nonnull final CloneContext cloneContext) {
    }
}
