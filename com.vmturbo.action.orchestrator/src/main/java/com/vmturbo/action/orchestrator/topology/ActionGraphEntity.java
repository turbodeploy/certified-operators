package com.vmturbo.action.orchestrator.topology;

import java.util.List;
import java.util.Optional;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;

import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageType;
import com.vmturbo.topology.graph.util.BaseGraphEntity;

/**
 * The topology entity representation in the action orchestrator. Contains only the essential
 * information that the action orchestrator needs to know.
 */
public class ActionGraphEntity extends BaseGraphEntity<ActionGraphEntity> {

    private final ActionEntityTypeSpecificInfo actionEntityInfo;

    private final IntList commsWithHotReplace;

    private ActionGraphEntity(@Nonnull final TopologyEntityDTO src) {
        super(src);

        this.actionEntityInfo = TopologyDTOUtil.makeActionTypeSpecificInfo(src)
                .map(ActionEntityTypeSpecificInfo.Builder::build)
                .orElse(null);
        final List<CommoditySoldDTO> commsToPersist = src.getCommoditySoldListList().stream()
                .filter(commSold -> {
                    final int commType = commSold.getCommodityType().getType();
                    return ActionDTOUtil.NON_DISRUPTIVE_SETTING_COMMODITIES.contains(commType);
                })
                .collect(Collectors.toList());
        if (commsToPersist.isEmpty()) {
            commsWithHotReplace = IntLists.EMPTY_LIST;
        } else {
            commsWithHotReplace = new IntArrayList(commsToPersist.size());
            commsToPersist.forEach(c -> commsWithHotReplace.add(c.getCommodityType().getType()));
        }
    }

    /**
     * Represent this entity as a {@link ActionPartialEntity} protobuf.
     * The protobuf is used for compatibility across plan and realtime. In plans the same
     * protobuf is retrieved from the repository.
     *
     * @return The {@link ActionPartialEntity} representation of the entity.
     */
    @Nonnull
    public ActionPartialEntity asPartialEntity() {
        // Information required by the action orchestrator.
        final ActionPartialEntity.Builder actionEntityBldr = ActionPartialEntity.newBuilder()
                .setOid(getOid())
                .setEntityType(getEntityType())
                .setDisplayName(getDisplayName());
        getDiscoveringTargetIds().forEach(actionEntityBldr::addDiscoveringTargetIds);
        commsWithHotReplace.forEach((IntConsumer)actionEntityBldr::addCommTypesWithHotReplace);
        List<Integer> providerEntityTypes = getProviders().stream().map(
                ActionGraphEntity::getEntityType).collect(Collectors.toList());
        Optional<Integer> primaryProviderIndex = TopologyDTOUtil.getPrimaryProviderIndex(
                getEntityType(), providerEntityTypes);
        primaryProviderIndex.ifPresent(index -> {
            long providerId = getProviders().get(index).getOid();
            actionEntityBldr.setPrimaryProviderId(providerId);
        });
        if (actionEntityInfo != null) {
            actionEntityBldr.setTypeSpecificInfo(actionEntityInfo);
        }
        actionEntityBldr.addAllConnectedEntities(getBroadcastConnections());

        if (getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE) {
            // If a vSAN storage consumes commodities from this entity then add a {@link ConnectedEntity}
            // to the list of its connections. One host can be part of only one vSAN storage.
            getConsumers().stream()
                .filter(c -> c.getEntityType() == EntityType.STORAGE_VALUE)
                .filter(c -> c.getActionEntityInfo().getStorage().getStorageType() == StorageType.VSAN)
                .map(consumer -> ConnectedEntity.newBuilder()
                        .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                        .setConnectedEntityType(consumer.getEntityType())
                        .setConnectedEntityId(consumer.getOid())
                        .build())
                .forEach(actionEntityBldr::addConnectedEntities);
        }
        return actionEntityBldr.build();
    }

    public ActionEntityTypeSpecificInfo getActionEntityInfo() {
        return actionEntityInfo;
    }

    /**
     * Builder for {@link ActionGraphEntity}.
     */
    public static class Builder extends BaseGraphEntity.Builder<Builder, ActionGraphEntity> {

        /**
         * Create a new entity builder.
         *
         * @param dto The {@link TopologyEntityDTO}.
         */
        public Builder(@Nonnull final TopologyEntityDTO dto) {
            super(dto, new ActionGraphEntity(dto));
        }
    }
}
