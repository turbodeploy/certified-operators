package com.vmturbo.repository.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithOnlyEnvironmentTypeAndTargets;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology;

/**
 * Responsible for converting entities in the repository to {@link PartialEntity}s with the
 * appropriate detail levels.
 */
public class PartialEntityConverter {

    private final LiveTopologyStore liveTopologyStore;

    /**
     * Constructor.
     *
     * @param liveTopologyStore The {@link LiveTopologyStore}.
     */
    public PartialEntityConverter(LiveTopologyStore liveTopologyStore) {
        this.liveTopologyStore = liveTopologyStore;
    }

    private Long2ObjectMap<Map<String, Set<String>>> getTagsByEntity(List<RepoGraphEntity> entities) {
        return liveTopologyStore.getSourceTopology()
            .map(SourceRealtimeTopology::globalTags)
            .map(globalTags -> {
                LongSet idSet = new LongOpenHashSet(entities.size());
                entities.forEach(e -> idSet.add(e.getOid()));
                return globalTags.getTagsByEntity(idSet);
            }).orElse(new Long2ObjectOpenHashMap<>());
    }

    /**
     * Create a {@link PartialEntity} from a {@link RepoGraphEntity}.
     *
     * @param inputStream Input stream of {@link RepoGraphEntity} to convert.
     * @param type The type of partial entity to return.
     * @param userSessionContext the user session context
     * @return Stream of converted entities.
     */
    @Nonnull
    public Stream<PartialEntity> createPartialEntities(@Nonnull final Stream<RepoGraphEntity> inputStream,
                                                       @Nonnull final Type type, @Nonnull final UserSessionContext userSessionContext) {

        final List<RepoGraphEntity> entities = inputStream.collect(Collectors.toList());
        final Long2ObjectMap<Map<String, Set<String>>> tagsByEntity = Type.API.equals(type) ? getTagsByEntity(entities) : null;
        return entities.stream()
                .map(repoGraphEntity -> {
                        Map<String, Set<String>> tagsForEntity = tagsByEntity == null ? new HashMap<>() : tagsByEntity.get(repoGraphEntity.getOid());
                        return createPartialEntity(repoGraphEntity, type, userSessionContext, tagsForEntity);
                        });
    }

    private RelatedEntity.Builder relatedEntity(@Nonnull final RepoGraphEntity repoGraphEntity) {
        return RelatedEntity.newBuilder()
            .setEntityType(repoGraphEntity.getEntityType())
            .setOid(repoGraphEntity.getOid())
            .setDisplayName(repoGraphEntity.getDisplayName());
    }

    /**
     * Create a {@link PartialEntity} from a {@link TopologyEntityDTO}.
     *
     * @param graphEntity Input entity.
     * @param type The type of partial entity to convert to.
     * @param userSessionContext the user session context.
     * @param tagsForEntity map containing tags by per entity
     * @return The converted {@link PartialEntity}.
     */
    @Nonnull
    public PartialEntity createPartialEntity(@Nonnull final RepoGraphEntity graphEntity,
            @Nonnull final Type type, @Nonnull final UserSessionContext userSessionContext,
            @Nullable Map<String, Set<String>> tagsForEntity) {
        PartialEntity.Builder partialEntityBldr = PartialEntity.newBuilder();
        switch (type) {
            case FULL:
                partialEntityBldr.setFullEntity(graphEntity.getTopologyEntity());
                break;
            case MINIMAL:
                // Minimal information to identify and display the entity.
                partialEntityBldr.setMinimal(MinimalEntity.newBuilder()
                        .setOid(graphEntity.getOid())
                        .setDisplayName(graphEntity.getDisplayName())
                        .setEntityType(graphEntity.getEntityType())
                        .setEnvironmentType(graphEntity.getEnvironmentType())
                        .setEntityState(graphEntity.getEntityState())
                        .addAllDiscoveringTargetIds(graphEntity.getDiscoveringTargetIds().collect(Collectors.toList())));
                break;
            case WITH_CONNECTIONS:
                partialEntityBldr.setWithConnections(graphEntity.asEntityWithConnections());
                break;
            case WITH_ONLY_ENVIRONMENT_TYPE_AND_TARGETS:
                partialEntityBldr.setWithOnlyEnvironmentTypeAndTargets(
                        EntityWithOnlyEnvironmentTypeAndTargets.newBuilder()
                                .setOid(graphEntity.getOid())
                                .setEnvironmentType(graphEntity.getEnvironmentType())
                                .addAllDiscoveringTargetIds(graphEntity.getDiscoveringTargetIds()
                                        .collect(Collectors.toList())));
                break;
            case API:
                // Information required by the API.
                final ApiPartialEntity.Builder apiBldr = ApiPartialEntity.newBuilder().setOid(
                        graphEntity.getOid()).setDisplayName(graphEntity.getDisplayName()).setEntityState(
                        graphEntity.getEntityState()).setEntityType(graphEntity.getEntityType()).setEnvironmentType(
                        graphEntity.getEnvironmentType()).setStale(graphEntity.isStale());
                graphEntity.getDiscoveringTargetIds().forEach(id -> {
                    String vendorId = graphEntity.getVendorId(id);
                    PerTargetEntityInformation.Builder info = PerTargetEntityInformation.newBuilder();
                    if (vendorId != null) {
                        info.setVendorId(vendorId);
                    }
                    apiBldr.putDiscoveredTargetData(id, info.build());
                });
                apiBldr.addAllConnectedTo(graphEntity.getBroadcastRelatedEntities());
                if (tagsForEntity != null) {
                    final Tags.Builder tags = Tags.newBuilder();
                    tagsForEntity.forEach((key, vals) -> {
                        tags.putTags(key, TagValuesDTO.newBuilder()
                                .addAllValues(vals)
                                .build());
                    });
                    apiBldr.setTags(tags);
                }
                graphEntity.getProviders().forEach(provider -> apiBldr.addProviders(relatedEntity(provider)));
                graphEntity.getConsumers().forEach(consumer -> apiBldr.addConsumers(relatedEntity(consumer)));
                partialEntityBldr.setApi(apiBldr);
                break;
            default:
                throw new IllegalArgumentException("Invalid partial entity type: " + type);
        }
        return applyScopeToPartialEntity(partialEntityBldr, type, userSessionContext);
    }

    /**
     * Create a {@link PartialEntity} from a {@link TopologyEntityDTO}.
     *
     * @param topoEntity Input entity.
     * @param type The type of partial entity to convert to.
     * @param userSessionContext the user session context.
     * @return The converted {@link PartialEntity}.
     */
    @Nonnull
    public PartialEntity createPartialEntity(@Nonnull final TopologyEntityDTOOrBuilder topoEntity,
                                             @Nonnull final PartialEntity.Type type,
                                             @Nonnull UserSessionContext userSessionContext) {
        PartialEntity.Builder partialEntityBldr = PartialEntity.newBuilder();
        switch (type) {
            case FULL:
                // The full topology entity.
                if (topoEntity instanceof TopologyEntityDTO) {
                    partialEntityBldr.setFullEntity((TopologyEntityDTO)topoEntity);
                } else if (topoEntity instanceof TopologyEntityDTO.Builder) {
                    partialEntityBldr.setFullEntity((TopologyEntityDTO.Builder)topoEntity);
                }
                break;
            case MINIMAL:
                // Minimal information to identify and display the entity.
                partialEntityBldr.setMinimal(MinimalEntity.newBuilder()
                    .setOid(topoEntity.getOid())
                    .setDisplayName(topoEntity.getDisplayName())
                    .setEntityType(topoEntity.getEntityType())
                    .setEnvironmentType(topoEntity.getEnvironmentType())
                    .setEntityState(topoEntity.getEntityState())
                    .addAllDiscoveringTargetIds(
                        topoEntity.getOrigin().getDiscoveryOrigin().getDiscoveredTargetDataMap().keySet()));
                break;
            case WITH_CONNECTIONS:
                final EntityWithConnections.Builder withConnectionsBuilder =
                    EntityWithConnections.newBuilder()
                        .setOid(topoEntity.getOid())
                        .setDisplayName(topoEntity.getDisplayName())
                        .setEntityType(topoEntity.getEntityType())
                        .addAllConnectedEntities(topoEntity.getConnectedEntityListList());
                partialEntityBldr.setWithConnections(withConnectionsBuilder);
                break;
            case ACTION:
                // Information required by the action orchestrator.
                final ActionPartialEntity.Builder actionEntityBldr = ActionPartialEntity.newBuilder()
                    .setOid(topoEntity.getOid())
                    .setEntityType(topoEntity.getEntityType())
                    .setDisplayName(topoEntity.getDisplayName())
                    .addAllDiscoveringTargetIds(
                        topoEntity.getOrigin().getDiscoveryOrigin().getDiscoveredTargetDataMap().keySet());
                topoEntity.getCommoditySoldListList().stream()
                    .filter(comm -> ActionDTOUtil.NON_DISRUPTIVE_SETTING_COMMODITIES.contains(
                        comm.getCommodityType().getType()))
                    .filter(comm -> comm.getHotResizeInfo().getHotReplaceSupported())
                    .mapToInt(comm -> comm.getCommodityType().getType())
                    .forEach(actionEntityBldr::addCommTypesWithHotReplace);
                List<Integer> providerEntityTypes = topoEntity.getCommoditiesBoughtFromProvidersList().stream()
                    .map(CommoditiesBoughtFromProvider::getProviderEntityType)
                    .collect(Collectors.toList());
                Optional<Integer> primaryProviderIndex = TopologyDTOUtil.getPrimaryProviderIndex(
                    topoEntity.getEntityType(), providerEntityTypes);
                primaryProviderIndex.ifPresent(index -> {
                    long primaryProviderId = topoEntity.getCommoditiesBoughtFromProvidersList().get(index).getProviderId();
                    actionEntityBldr.setPrimaryProviderId(primaryProviderId);
                });
                TopologyDTOUtil.makeActionTypeSpecificInfo(topoEntity)
                    .ifPresent(actionEntityBldr::setTypeSpecificInfo);
                partialEntityBldr.setAction(actionEntityBldr);
                break;
            case API:
                // Information required by the API.
                final ApiPartialEntity.Builder apiBldr = ApiPartialEntity.newBuilder()
                    .setOid(topoEntity.getOid())
                    .setDisplayName(topoEntity.getDisplayName())
                    .setEntityState(topoEntity.getEntityState())
                    .setEntityType(topoEntity.getEntityType())
                    .setEnvironmentType(topoEntity.getEnvironmentType())
                    .setTags(topoEntity.getTags())
                    .putAllDiscoveredTargetData(topoEntity.getOrigin()
                                    .getDiscoveryOrigin().getDiscoveredTargetDataMap());

                topoEntity.getConnectedEntityListList().forEach(connectedEntity ->
                        apiBldr.addConnectedTo(RelatedEntity.newBuilder()
                                                .setOid(connectedEntity.getConnectedEntityId())
                                                .setEntityType(connectedEntity.getConnectedEntityType())));
                topoEntity.getCommoditiesBoughtFromProvidersList().forEach(commBoughtFromProvider ->
                        apiBldr.addProviders(RelatedEntity.newBuilder()
                                                .setOid(commBoughtFromProvider.getProviderId())
                                                .setEntityType(commBoughtFromProvider
                                                                    .getProviderEntityType())
                                                .build()));
                partialEntityBldr.setApi(apiBldr);
                break;
            default:
                throw new IllegalArgumentException("Invalid partial entity type: " + type);
        }
        return applyScopeToPartialEntity(partialEntityBldr, type, userSessionContext);
    }

    /**
     * Filter out connected entities, providers, consumers that are not part of the scope.
     * @param partialEntityBldr the partial entity builder
     * @param type the type of entity requested
     * @param userSessionContext the user session context
     * @return a {@link PartialEntity}
     */
    private PartialEntity applyScopeToPartialEntity(@Nonnull final PartialEntity.Builder partialEntityBldr,
                                                    @Nonnull final Type type,
                                                    UserSessionContext userSessionContext) {
        if (userSessionContext.isUserScoped()) {
            final Predicate<ConnectedEntity> connectedEntityScopeFilter = entity -> userSessionContext.getUserAccessScope().contains(entity.getConnectedEntityId());
            final Predicate<RelatedEntity> relatedEntityScopeFilter = entity -> userSessionContext.getUserAccessScope().contains(entity.getOid());
            switch (type) {
                case FULL:
                    List<ConnectedEntity> unfilteredConnectedEntities =
                            partialEntityBldr.getFullEntity().getConnectedEntityListList();
                    TopologyEntityDTO.Builder topologyDtoBuilder = partialEntityBldr.getFullEntity()
                            .toBuilder()
                            .clearConnectedEntityList();
                    List<ConnectedEntity> filteredEntities = unfilteredConnectedEntities.stream()
                            .filter(connectedEntityScopeFilter).collect(Collectors.toList());
                    topologyDtoBuilder.addAllConnectedEntityList(filteredEntities).build();
                    partialEntityBldr.setFullEntity(topologyDtoBuilder);
                    break;
                case WITH_CONNECTIONS:
                    unfilteredConnectedEntities =
                            partialEntityBldr.getWithConnections().getConnectedEntitiesList();
                    EntityWithConnections.Builder entityWithConnectionsBuilder =
                            partialEntityBldr.getWithConnections().toBuilder().clearConnectedEntities();
                    filteredEntities = unfilteredConnectedEntities.stream().filter(connectedEntityScopeFilter).collect(
                            Collectors.toList());
                    entityWithConnectionsBuilder.addAllConnectedEntities(filteredEntities);
                    partialEntityBldr.setWithConnections(entityWithConnectionsBuilder);
                    break;
                case API:
                    List<RelatedEntity> unfilteredProviders =
                            partialEntityBldr.getApi().getProvidersList();
                    List<RelatedEntity> unfilteredConsumers =
                            partialEntityBldr.getApi().getConsumersList();
                    List<RelatedEntity> unfilteredConnected =
                            partialEntityBldr.getApi().getConnectedToList();

                    ApiPartialEntity.Builder builder = partialEntityBldr.getApi()
                            .toBuilder()
                            .clearConnectedTo()
                            .clearConsumers()
                            .clearProviders();
                    List<RelatedEntity> filteredProviders = unfilteredProviders.stream().filter(relatedEntityScopeFilter)
                            .collect(Collectors.toList());
                    builder.addAllProviders(filteredProviders);
                    List<RelatedEntity> filteredConsumers = unfilteredConsumers.stream().filter(relatedEntityScopeFilter)
                            .collect(Collectors.toList());
                    builder.addAllConsumers(filteredConsumers);
                    List<RelatedEntity> filteredConnected = unfilteredConnected.stream().filter(relatedEntityScopeFilter)
                            .collect(Collectors.toList());
                    builder.addAllConnectedTo(filteredConnected);
                    partialEntityBldr.setApi(builder);
                    break;
                case ACTION:
                    unfilteredConnectedEntities =
                            partialEntityBldr.getAction().getConnectedEntitiesList();
                    ActionPartialEntity.Builder actionEntityBuilder =
                            partialEntityBldr.getAction().toBuilder().clearConnectedEntities();
                    filteredEntities = unfilteredConnectedEntities.stream().filter(connectedEntityScopeFilter).collect(
                            Collectors.toList());
                    actionEntityBuilder.addAllConnectedEntities(filteredEntities).build();
                    partialEntityBldr.setAction(actionEntityBuilder);
                    break;
                default:
                    break;
            }
        }
        return partialEntityBldr.build();
    }
}
