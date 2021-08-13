package com.vmturbo.topology.processor.planexport;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.platform.common.builders.SDKConstants;
import com.vmturbo.platform.common.dto.ActionExecution;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ProviderInfo;
import com.vmturbo.platform.common.dto.CommonDTO.ContextData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.actions.data.EntityRetrievalException;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;

/**
 * The class to convert move actionDTO to executionDTO.
 */
public class MoveExecutionDTOConverter implements ExecutionDTOConverter {
    private static final Logger logger = LogManager.getLogger();

    private EntityRetriever entityRetriever;

    /**
     * Constructor.
     *
     * @param entityRetriever the object retrieves and converts an entity.
     */
    public MoveExecutionDTOConverter(EntityRetriever entityRetriever) {
        this.entityRetriever = entityRetriever;
    }

    @NotNull
    @Override
    public ActionExecutionDTO convert(@NotNull Action action) throws ActionDTOConversionException {
        try {
            Move move = action.getInfo().getMove();
            ActionExecutionDTO.Builder executionDTO = ActionExecutionDTO.newBuilder()
                    .setActionType(ActionType.MOVE).setActionOid(action.getId());
            TopologyEntityDTO targetDTO = entityRetriever.retrieveTopologyEntity(move.getTarget().getId())
                .orElseThrow(() -> new EntityRetrievalException("No entity found for id "
                    + move.getTarget().getId()));
            EntityDTO target = entityRetriever.convertToEntityDTO(targetDTO);
            for (ChangeProvider change: move.getChangesList()) {
                EntityDTO source = entityRetriever.fetchAndConvertToEntityDTO(
                        change.getSource().getId());
                EntityDTO destination = entityRetriever.fetchAndConvertToEntityDTO(
                        change.getDestination().getId());
                ActionItemDTO.Builder actionBuilder = ActionExecution.ActionItemDTO.newBuilder()
                        .setActionType(ActionType.MOVE)
                        .setUuid(Long.toString(action.getId()))
                        .setTargetSE(target)
                        .setCurrentSE(source)
                        .setNewSE(destination);
                Map<EntityType, Set<String>> resourceIds = new HashMap();
                // Find the resource entityDTO ids which are essentially the ids stored in the
                // EntityDTO's VirtualMachineData diskToStorage map.
                for (ActionEntity resourceEntity : change.getResourceList()) {
                    EntityDTO resourceDTO = entityRetriever.fetchAndConvertToEntityDTO(resourceEntity.getId());
                    Set<String> ids = resourceIds.get(resourceDTO.getEntityType());
                    if (ids == null) {
                        ids = new HashSet();
                        resourceIds.put(resourceDTO.getEntityType(), ids);
                    }
                    ids.add(resourceDTO.getId());
                }
                for (Map.Entry<EntityType, Set<String>> entry : resourceIds.entrySet()) {
                    actionBuilder.addProviders(ProviderInfo.newBuilder()
                            .setEntityType(entry.getKey()).addAllIds(entry.getValue()).build());
                }
                addContextData(actionBuilder, targetDTO);
                executionDTO.addActionItem(actionBuilder.build());
            }
            return executionDTO.build();
        } catch (Exception e) {
            throw new ActionDTOConversionException(e.getMessage(), e);
        }
    }

    private void addContextData(ActionItemDTO.Builder builder, TopologyEntityDTO target)
        throws EntityRetrievalException {
        for (ConnectedEntity connection : target.getConnectedEntityListList()) {
            if (connection.getConnectionType() == ConnectionType.AGGREGATED_BY_CONNECTION
                && connection.getConnectedEntityType() == EntityType.HYPERVISOR_SERVER_VALUE) {
                TopologyEntityDTO hypervisorServer = entityRetriever.retrieveTopologyEntity(
                    connection.getConnectedEntityId()).orElseThrow(
                    () -> new EntityRetrievalException("No entity found for hypervisor server "
                        + connection.getConnectedEntityId()));

                if (hypervisorServer.hasOrigin() && hypervisorServer.getOrigin().hasDiscoveryOrigin()) {
                    List<String> vendorIds = hypervisorServer.getOrigin().getDiscoveryOrigin()
                        .getDiscoveredTargetDataMap().values().stream()
                        .filter(PerTargetEntityInformation::hasVendorId)
                        .map(PerTargetEntityInformation::getVendorId)
                        .distinct()
                        .collect(Collectors.toList());

                    if (vendorIds.size() > 0) {
                        if (vendorIds.size() > 1) {
                            logger.warn("Hypervisor Server \"{}\" with oid {} has {} different vendor"
                                    + " IDs, choosing \"{}\"",
                                hypervisorServer.getDisplayName(), hypervisorServer.getOid(),
                                vendorIds.size(), vendorIds.get(0));
                        }

                        builder.addContextData(ContextData.newBuilder()
                            .setContextKey(SDKConstants.HYPERVISOR_UUID)
                            .setContextValue(vendorIds.get(0))
                            .build());
                    } else {
                        logger.warn("Hypervisor Server \"{}\" with oid {} has no vendor"
                                + " ID, skipping addition of context data",
                            hypervisorServer.getDisplayName(), hypervisorServer.getOid());
                    }
                }
            }
        }
    }
}
