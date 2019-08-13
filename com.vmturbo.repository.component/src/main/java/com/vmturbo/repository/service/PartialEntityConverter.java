package com.vmturbo.repository.service;

import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;

/**
 * Responsible for converting entities in the repository to {@link PartialEntity}s with the
 * appropriate detail levels.
 */
public class PartialEntityConverter {

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
            .setOid(repoGraphEntity.getOid());
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
