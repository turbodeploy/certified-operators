package com.vmturbo.repository.service;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.TypeSpecificPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;

/**
 * Responsible for converting entities in the repository to {@link PartialEntity}s with the
 * appropriate detail levels.
 */
public class PartialEntityConverter {

    private final Logger logger = LogManager.getLogger();

    /**
     * Create a {@link PartialEntity} from a {@link RepoGraphEntity}.
     */
    @Nonnull
    public PartialEntity createPartialEntity(@Nonnull final RepoGraphEntity repoGraphEntity,
                                             @Nonnull final PartialEntity.Type type) {
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
                    .addAllDiscoveringTargetIds(
                        repoGraphEntity.getDiscoveringTargetIds().collect(Collectors.toList())));
                break;
            case TYPE_SPECIFIC:
                final TypeSpecificPartialEntity.Builder typeSpecificBuilder =
                    TypeSpecificPartialEntity.newBuilder()
                        .setOid(repoGraphEntity.getOid())
                        .setDisplayName(repoGraphEntity.getDisplayName())
                        .setEntityType(repoGraphEntity.getEntityType())
                        .setTypeSpecificInfo(repoGraphEntity.getTypeSpecificInfo());
                partialEntityBldr.setTypeSpecific(typeSpecificBuilder);
                break;
            case WITH_CONNECTIONS:
                final EntityWithConnections.Builder withConnectionsBuilder =
                    EntityWithConnections.newBuilder()
                        .setOid(repoGraphEntity.getOid())
                        .setDisplayName(repoGraphEntity.getDisplayName());
                repoGraphEntity.getConnectedToEntitiesByConnectionType()
                    .forEach((connectionType, connectedEntities) -> {
                        connectedEntities.forEach(connectedEntity -> {
                            withConnectionsBuilder.addConnectedEntities(ConnectedEntity.newBuilder()
                                .setConnectedEntityId(connectedEntity.getOid())
                                .setConnectedEntityType(connectedEntity.getEntityType())
                                .setConnectionType(connectionType));
                        });
                    });
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
                ActionDTOUtil.NON_DISRUPTIVE_SETTING_COMMODITIES.forEach(commType -> {
                    final CommoditySoldDTO comm = repoGraphEntity.soldCommoditiesByType().get(commType);
                    if (comm != null) {
                        actionEntityBldr.addCommoditySold(comm);
                    }
                });
                List<Integer> providerEntityTypes = repoGraphEntity.getProviders().stream()
                    .map(RepoGraphEntity::getEntityType).collect(Collectors.toList());
                Optional<Integer> primaryProviderIndex = TopologyDTOUtil.getPrimaryProviderIndex(
                    repoGraphEntity.getEntityType(), repoGraphEntity.getOid(), providerEntityTypes);
                primaryProviderIndex.ifPresent(index -> {
                    long providerId = repoGraphEntity.getProviders().get(index).getOid();
                    actionEntityBldr.setPrimaryProviderId(providerId);
                });
                actionEntityBldr.setTypeSpecificInfo(repoGraphEntity.getTypeSpecificInfo());
                partialEntityBldr.setAction(actionEntityBldr);
                break;
            case API:
                // Information required by the API.
                final Tags.Builder tagsBldr = Tags.newBuilder();
                repoGraphEntity.getTags().forEach((key, vals) -> tagsBldr.putTags(key, TagValuesDTO.newBuilder()
                    .addAllValues(vals)
                    .build()));
                final ApiPartialEntity.Builder apiBldr = ApiPartialEntity.newBuilder()
                    .setOid(repoGraphEntity.getOid())
                    .setDisplayName(repoGraphEntity.getDisplayName())
                    .setEntityState(repoGraphEntity.getEntityState())
                    .setEntityType(repoGraphEntity.getEntityType())
                    .setEnvironmentType(repoGraphEntity.getEnvironmentType())
                    .setTags(tagsBldr);
                repoGraphEntity.getDiscoveringTargetIds().forEach(apiBldr::addDiscoveringTargetIds);
                repoGraphEntity.getConnectedToEntities().forEach(connectedTo -> {
                    apiBldr.addConnectedTo(relatedEntity(connectedTo));
                });
                repoGraphEntity.getProviders().forEach(provider -> {
                    apiBldr.addProviders(relatedEntity(provider));
                });
                partialEntityBldr.setApi(apiBldr);
                break;
            default:
                throw new IllegalArgumentException("Invalid partial entity type: " + type);
        }
        return partialEntityBldr.build();
    }

    private RelatedEntity.Builder relatedEntity(@Nonnull final RepoGraphEntity repoGraphEntity) {
        return RelatedEntity.newBuilder()
            .setEntityType(repoGraphEntity.getEntityType())
            .setOid(repoGraphEntity.getOid())
            .setDisplayName(repoGraphEntity.getDisplayName());
    }

    /**
     * Create a {@link PartialEntity} from a {@link TopologyEntityDTO}.
     */
    @Nonnull
    public PartialEntity createPartialEntity(@Nonnull final TopologyEntityDTO topoEntity,
                                             @Nonnull final PartialEntity.Type type) {
        PartialEntity.Builder partialEntityBldr = PartialEntity.newBuilder();
        switch (type) {
            case FULL:
                // The full topology entity.
                partialEntityBldr.setFullEntity(topoEntity);
                break;
            case MINIMAL:
                // Minimal information to identify and display the entity.
                partialEntityBldr.setMinimal(MinimalEntity.newBuilder()
                    .setOid(topoEntity.getOid())
                    .setDisplayName(topoEntity.getDisplayName())
                    .setEntityType(topoEntity.getEntityType())
                    .setEnvironmentType(topoEntity.getEnvironmentType())
                    .addAllDiscoveringTargetIds(
                        topoEntity.getOrigin().getDiscoveryOrigin().getDiscoveringTargetIdsList()));
                break;
            case TYPE_SPECIFIC:
                final TypeSpecificPartialEntity.Builder typeSpecificBuilder =
                    TypeSpecificPartialEntity.newBuilder()
                        .setOid(topoEntity.getOid())
                        .setDisplayName(topoEntity.getDisplayName())
                        .setEntityType(topoEntity.getEntityType())
                        .setTypeSpecificInfo(topoEntity.getTypeSpecificInfo());
                partialEntityBldr.setTypeSpecific(typeSpecificBuilder);
                break;
            case WITH_CONNECTIONS:
                final EntityWithConnections.Builder withConnectionsBuilder =
                    EntityWithConnections.newBuilder()
                        .setOid(topoEntity.getOid())
                        .setDisplayName(topoEntity.getDisplayName())
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
                        topoEntity.getOrigin().getDiscoveryOrigin().getDiscoveringTargetIdsList());
                topoEntity.getCommoditySoldListList().stream()
                    .filter(comm -> ActionDTOUtil.NON_DISRUPTIVE_SETTING_COMMODITIES.contains(
                        comm.getCommodityType().getType()))
                    .forEach(actionEntityBldr::addCommoditySold);
                List<Integer> providerEntityTypes = topoEntity.getCommoditiesBoughtFromProvidersList().stream()
                    .map(commBoughtFromProvider -> commBoughtFromProvider.getProviderEntityType())
                    .collect(Collectors.toList());
                Optional<Integer> primaryProviderIndex = TopologyDTOUtil.getPrimaryProviderIndex(
                    topoEntity.getEntityType(), topoEntity.getOid(), providerEntityTypes);
                primaryProviderIndex.ifPresent(index -> {
                    long primaryProviderId = topoEntity.getCommoditiesBoughtFromProvidersList().get(index).getProviderId();
                    actionEntityBldr.setPrimaryProviderId(primaryProviderId);
                });
                actionEntityBldr.setTypeSpecificInfo(topoEntity.getTypeSpecificInfo());
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
                    .setTags(topoEntity.getTags());
                topoEntity.getOrigin().getDiscoveryOrigin().getDiscoveringTargetIdsList()
                    .forEach(apiBldr::addDiscoveringTargetIds);

                topoEntity.getConnectedEntityListList().forEach(connectedEntity -> {
                    apiBldr.addConnectedTo(RelatedEntity.newBuilder()
                        .setOid(connectedEntity.getConnectedEntityId())
                        .setEntityType(connectedEntity.getConnectedEntityType()));
                });
                topoEntity.getCommoditiesBoughtFromProvidersList().forEach(commBoughtFromProvider -> {
                    apiBldr.addProviders(RelatedEntity.newBuilder()
                        .setOid(commBoughtFromProvider.getProviderId())
                        .setEntityType(commBoughtFromProvider.getProviderEntityType())
                        .build());
                });
                partialEntityBldr.setApi(apiBldr);
                break;
            default:
                throw new IllegalArgumentException("Invalid partial entity type: " + type);
        }
        return partialEntityBldr.build();
    }
}
