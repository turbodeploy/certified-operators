package com.vmturbo.topology.processor.actions.data.spec;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.builders.SDKConstants;
import com.vmturbo.platform.common.dto.CommonDTO.ContextData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.GroupAndPolicyRetriever;

/**
 * This adds cluster data for VM and PM entities.
 */
public class EntityClusterDataSpec implements DataRequirementSpec {
    private static Logger logger = LogManager.getLogger();

    private final EntityRetriever entityRetriever;
    private final GroupAndPolicyRetriever groupAndPolicyRetriever;

    /**
     * Creates a new instance onf this class.
     *
     * @param entityRetriever the object for retrieving objects.
     * @param groupAndPolicyRetriever the object for retrieving groups.
     */
    public EntityClusterDataSpec(@Nonnull EntityRetriever entityRetriever,
                                 @Nonnull GroupAndPolicyRetriever groupAndPolicyRetriever) {
        this.entityRetriever = Objects.requireNonNull(entityRetriever);
        this.groupAndPolicyRetriever = Objects.requireNonNull(groupAndPolicyRetriever);
    }


    @Override
    public boolean matchesAllCriteria(@Nonnull ActionInfo action) {
        final ActionEntity entity = getActionTargetEntity(action);

        return entity != null
            && entity.getEnvironmentType() == EnvironmentTypeEnum.EnvironmentType.ON_PREM
            && (entity.getType() == EntityType.PHYSICAL_MACHINE.getNumber()
            || entity.getType() == EntityType.VIRTUAL_MACHINE.getNumber());
    }

    @Nonnull
    @Override
    public List<ContextData> retrieveRequiredData(@Nonnull ActionInfo action) {
        List<ContextData> contextData = new ArrayList<>();
        contextData.addAll(getCurrentClusterData(action));
        contextData.addAll(getNewClusterData(action));
        return contextData;
    }

    @Nullable
    private ActionEntity getActionTargetEntity(@Nonnull ActionInfo actionInfo) {
        try {
            return ActionDTOUtil.getPrimaryEntity(0L, actionInfo, false);
        } catch (UnsupportedActionException ex) {
            logger.error("Unsupported action type while getting the primary entity for \"{}\"",
                actionInfo, ex);
            return null;
        }
    }

    @Nonnull
    private List<ContextData> getCurrentClusterData(@Nonnull ActionInfo action) {
        List<ContextData> clusterData = new ArrayList<>();
        final ActionEntity entity = getActionTargetEntity(action);
        if (entity == null) {
            logger.error("Cannot determine the target entity for action \"{}\".", action);
            return clusterData;
        }
        final long physicalMachineId;
        if (entity.getType() == EntityType.VIRTUAL_MACHINE.getNumber()) {
            TopologyEntityDTO vm =
                entityRetriever.retrieveTopologyEntity(entity.getId()).orElse(null);
            if (vm == null) {
                logger.error("Cannot lookup the target entity for action \"{}\".", action);
                return clusterData;
            }

            Long pmId = vm.getCommoditiesBoughtFromProvidersList()
                .stream()
                .filter(c -> c.getProviderEntityType() == EntityType.PHYSICAL_MACHINE.getNumber())
                .map(TopologyEntityDTO.CommoditiesBoughtFromProvider::getProviderId)
                .findAny().orElse(null);

            if (pmId == null) {
                logger.error("Cannot find the hosting PM for target VM of action {}.", action);
                return clusterData;
            }
            physicalMachineId = pmId;
        } else {
            physicalMachineId = entity.getId();
        }

        GroupDTO.Grouping cluster = groupAndPolicyRetriever.getHostCluster(physicalMachineId).orElse(null);

        if (cluster != null) {
            clusterData.add(ContextData.newBuilder().setContextKey(SDKConstants.CURRENT_HOST_CLUSTER_ID)
              .setContextValue(String.valueOf(cluster.getId())).build());
            clusterData.add(ContextData.newBuilder().setContextKey(SDKConstants.CURRENT_HOST_CLUSTER_DISPLAY_NAME)
                .setContextValue(cluster.getDefinition().getDisplayName()).build());
        }

        return clusterData;
    }

    @Nonnull
    private List<ContextData> getNewClusterData(@Nonnull ActionInfo action) {
        List<ContextData> clusterData = new ArrayList<>();
        if (action.getActionTypeCase() == ActionInfo.ActionTypeCase.MOVE) {
            Long newPmId = action.getMove().getChangesList()
                .stream()
                .map(ActionDTO.ChangeProvider::getDestination)
                .filter(c -> c.getType() == EntityType.PHYSICAL_MACHINE.getNumber())
                .map(ActionEntity::getId)
                .findAny().orElse(null);
            if (newPmId != null) {
                GroupDTO.Grouping cluster = groupAndPolicyRetriever.getHostCluster(newPmId).orElse(null);

                if (cluster != null) {
                    clusterData.add(ContextData.newBuilder().setContextKey(SDKConstants.NEW_HOST_CLUSTER_ID)
                        .setContextValue(String.valueOf(cluster.getId())).build());
                    clusterData.add(ContextData.newBuilder().setContextKey(SDKConstants.NEW_HOST_CLUSTER_DISPLAY_NAME)
                        .setContextValue(cluster.getDefinition().getDisplayName()).build());
                }
            }
        }

        return clusterData;
    }


}
