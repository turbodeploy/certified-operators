package com.vmturbo.repository.service;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageType;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity.SoldCommodity;
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
     * @return Stream of converted entities.
     */
    @Nonnull
    public Stream<PartialEntity> createPartialEntities(@Nonnull final Stream<RepoGraphEntity> inputStream,
                                                       @Nonnull final PartialEntity.Type type) {
        if (Type.API.equals(type)) {
            List<RepoGraphEntity> entities = inputStream.collect(Collectors.toList());
            Long2ObjectMap<Map<String, Set<String>>> tagsByEntity = getTagsByEntity(entities);
            return entities.stream()
                .map(repoGraphEntity -> {
                    final PartialEntity.Builder partialEntityBldr = PartialEntity.newBuilder();
                    // Information required by the API.
                    final ApiPartialEntity.Builder apiBldr = ApiPartialEntity.newBuilder().setOid(
                            repoGraphEntity.getOid()).setDisplayName(repoGraphEntity.getDisplayName()).setEntityState(
                            repoGraphEntity.getEntityState()).setEntityType(repoGraphEntity.getEntityType()).setEnvironmentType(
                            repoGraphEntity.getEnvironmentType());
                    Map<String, Set<String>> tagsForEntity = tagsByEntity.get(repoGraphEntity.getOid());
                    if (tagsForEntity != null) {
                        final Tags.Builder tags = Tags.newBuilder();
                        tagsForEntity.forEach((key, vals) -> {
                            tags.putTags(key, TagValuesDTO.newBuilder()
                                .addAllValues(vals)
                                .build());
                        });
                        apiBldr.setTags(tags);
                    }
                    repoGraphEntity.getDiscoveringTargetIds().forEach(id -> {
                        String vendorId = repoGraphEntity.getVendorId(id);
                        PerTargetEntityInformation.Builder info = PerTargetEntityInformation.newBuilder();
                        if (vendorId != null) {
                            info.setVendorId(vendorId);
                        }
                        apiBldr.putDiscoveredTargetData(id, info.build());
                    });
                    apiBldr.addAllConnectedTo(repoGraphEntity.getBroadcastRelatedEntities());
                    repoGraphEntity.getProviders().forEach(provider -> apiBldr.addProviders(relatedEntity(provider)));
                    repoGraphEntity.getConsumers().forEach(consumer -> apiBldr.addConsumers(relatedEntity(consumer)));
                    partialEntityBldr.setApi(apiBldr);
                    return partialEntityBldr.build();
            });
        } else {
            return inputStream.map(repoGraphEntity -> {
                PartialEntity.Builder partialEntityBldr = PartialEntity.newBuilder();
                switch (type) {
                    case FULL:
                        // The full topology entity.
                        partialEntityBldr.setFullEntity(repoGraphEntity.getTopologyEntity());
                        break;
                    case MINIMAL:
                        // Minimal information to identify and display the entity.
                        partialEntityBldr.setMinimal(MinimalEntity.newBuilder()
                                .setOid(repoGraphEntity.getOid())
                                .setDisplayName(repoGraphEntity.getDisplayName())
                                .setEntityType(repoGraphEntity.getEntityType())
                                .setEnvironmentType(repoGraphEntity.getEnvironmentType())
                                .setEntityState(repoGraphEntity.getEntityState())
                                .addAllDiscoveringTargetIds(
                                        repoGraphEntity.getDiscoveringTargetIds().collect(Collectors.toList())));
                        break;
                    case WITH_CONNECTIONS:
                        final EntityWithConnections.Builder withConnectionsBuilder =
                                EntityWithConnections.newBuilder()
                                        .setOid(repoGraphEntity.getOid())
                                        .setEntityType(repoGraphEntity.getEntityType())
                                        .setDisplayName(repoGraphEntity.getDisplayName());
                        withConnectionsBuilder.addAllConnectedEntities(repoGraphEntity.getBroadcastConnections());
                        partialEntityBldr.setWithConnections(withConnectionsBuilder);
                        break;
                    case ACTION:
                        // Information required by the action orchestrator.
                        final ActionPartialEntity.Builder actionEntityBldr = ActionPartialEntity.newBuilder()
                                .setOid(repoGraphEntity.getOid())
                                .setEntityType(repoGraphEntity.getEntityType())
                                .setDisplayName(repoGraphEntity.getDisplayName())
                                .addAllDiscoveringTargetIds(
                                        repoGraphEntity.getDiscoveringTargetIds().collect(Collectors.toList()));
                        repoGraphEntity.soldCommoditiesByType(new IntOpenHashSet(ActionDTOUtil.NON_DISRUPTIVE_SETTING_COMMODITIES)).filter(
                                SoldCommodity::isSupportsHotReplace).mapToInt(SoldCommodity::getType).forEach(
                                actionEntityBldr::addCommTypesWithHotReplace);
                        List<Integer> providerEntityTypes = repoGraphEntity.getProviders().stream().map(
                                RepoGraphEntity::getEntityType).collect(Collectors.toList());
                        Optional<Integer> primaryProviderIndex = TopologyDTOUtil.getPrimaryProviderIndex(
                                repoGraphEntity.getEntityType(), repoGraphEntity.getOid(),
                                providerEntityTypes);
                        primaryProviderIndex.ifPresent(index -> {
                            long providerId = repoGraphEntity.getProviders().get(index).getOid();
                            actionEntityBldr.setPrimaryProviderId(providerId);
                        });
                        ActionEntityTypeSpecificInfo typeSpecificInfo = repoGraphEntity.getActionTypeSpecificInfo();
                        if (typeSpecificInfo != null) {
                            actionEntityBldr.setTypeSpecificInfo(typeSpecificInfo);
                        }
                        actionEntityBldr.addAllConnectedEntities(repoGraphEntity.getBroadcastConnections());
                        addHostToVSANStorageConnection(repoGraphEntity, actionEntityBldr);
                        partialEntityBldr.setAction(actionEntityBldr);
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid partial entity type: " + type);
                }
                return partialEntityBldr.build();
            });
        }
    }

    /**
     * If a vSAN storage consumes commodities from this entity then add a {@link ConnectedEntity}
     * to the list of its connections. One host can be part of only one vSAN storage.
     * @param repoGraphEntity   the entity for which we want to know whether it sells commodities to a vSAN storage
     * @param actionEntityBldr  the action partial entity builder to which we need to put info about the connected vSAN storage
     */
    private void addHostToVSANStorageConnection(@Nonnull final RepoGraphEntity repoGraphEntity,
                    @Nonnull final ActionPartialEntity.Builder actionEntityBldr)  {
        if (repoGraphEntity.getEntityType() != EntityType.PHYSICAL_MACHINE_VALUE)   {
            //we care only about hosts here
            return;
        }
        for (RepoGraphEntity consumer : repoGraphEntity.getConsumers())    {
            if (consumer.getEntityType() == EntityType.STORAGE_VALUE
                    && consumer.getActionTypeSpecificInfo() != null
                    && consumer.getActionTypeSpecificInfo().getStorage().getStorageType() == StorageType.VSAN) {
                ConnectedEntity vsanConnectedEntity = ConnectedEntity.newBuilder()
                        .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                        .setConnectedEntityType(consumer.getEntityType())
                        .setConnectedEntityId(consumer.getOid())
                        .build();
                actionEntityBldr.addConnectedEntities(vsanConnectedEntity);
                return;
            }
        }
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
     * @param topoEntity Input entity.
     * @param type The type of partial entity to convert to.
     * @return The converted {@link PartialEntity}.
     */
    @Nonnull
    public PartialEntity createPartialEntity(@Nonnull final TopologyEntityDTOOrBuilder topoEntity,
                                             @Nonnull final PartialEntity.Type type) {
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
                    topoEntity.getEntityType(), topoEntity.getOid(), providerEntityTypes);
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
        return partialEntityBldr.build();
    }
}
