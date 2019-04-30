package com.vmturbo.action.orchestrator.execution;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.grpc.StatusRuntimeException;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupPropertyFilterList;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

/**
 * FailedCloudVMGroupProcessor processes actionEvents and updates the failed group with new vm entities.
 * If an action fails on a VM, it gets added to the group. If action succeeds it is removed from the group.
 */

public class FailedCloudVMGroupProcessor {

    private final static Logger logger = LogManager.getLogger(FailedCloudVMGroupProcessor.class);
    /**
     * Name of the group that we will create and manage in group component.
     */
    private final static String FAILED_GROUP_CLOUD_VMS = "Cloud VMs with Failed Sizing";
    private final GroupServiceBlockingStub groupServiceClient;
    private final Object lock = new Object();

    @GuardedBy("lock")
    private Set<Long> successOidsSet = new HashSet<>();
    @GuardedBy("lock")
    private Set<Long> failedOidsSet = new HashSet<>();

    public FailedCloudVMGroupProcessor(final GroupServiceBlockingStub groupServiceClient, final ScheduledExecutorService scheduledExecutorService,
                                       final int groupUpdateDelaySeconds) {
        this.groupServiceClient = groupServiceClient;
        scheduledExecutorService.scheduleWithFixedDelay(this::processAndUpdateGroup,
                0,
                groupUpdateDelaySeconds,
                TimeUnit.SECONDS);
    }

    /**
     * Processes the next item in processingQueue.
     * create/Updates FAILED_GROUP_CLOUD_VMS if it does not exist or with new virtual machine ID using groupService
     */
    private void processAndUpdateGroup() {
        final Set<Long> currSuccessOids;
        final Set<Long> currFailedOids;
        final Group group;
        try {
            if (successOidsSet.isEmpty() && failedOidsSet.isEmpty()) {
                logger.debug("No new entities to process.");
                return;
            }

            group = getFailedGroup().orElseGet(() -> createFailedActionGroup());
            Preconditions.checkArgument(group != null && group.hasGroup(), "Failed group not found or was not created");
        } catch (StatusRuntimeException e) {
            logger.error("Error while fetching/creating failed group using group service. There are {} items still to be processed", (successOidsSet.size() + failedOidsSet.size()), e);
            return;
        }
        synchronized (lock) {
            currSuccessOids = successOidsSet;
            currFailedOids = failedOidsSet;
            successOidsSet = new HashSet<>();
            failedOidsSet = new HashSet<>();
        }

        try {
            GroupInfo groupInfo = group.getGroup();
            Set<Long> memberOidsSet = new HashSet<>(groupInfo
                    .getStaticGroupMembersOrBuilder()
                    .getStaticMemberOidsList());

            boolean oidsAdded = memberOidsSet.addAll(currFailedOids);
            boolean oidsRemoved = memberOidsSet.removeAll(currSuccessOids);

            if (oidsAdded || oidsRemoved) {
                GroupInfo newInfo = groupInfo.toBuilder()
                        .setStaticGroupMembers(StaticGroupMembers.newBuilder().addAllStaticMemberOids(memberOidsSet))
                        .build();
                groupServiceClient.updateGroup(UpdateGroupRequest.newBuilder()
                        .setId(group.getId())
                        .setNewInfo(newInfo).build());
                logger.debug("Updated group: {} successfully with {} entities ", FAILED_GROUP_CLOUD_VMS, (currFailedOids.size() + currSuccessOids.size()));
            } else {
                logger.debug("memberOidsSet did not change. Not sending updateGroupRequest");
            }

        } catch (StatusRuntimeException e) {
            //add everything back to respective sets
            restoreGlobalSets(currSuccessOids, currFailedOids);
            logger.error("Error while updating failed group with vm entities. There are {} items still to be processed", (successOidsSet.size() + failedOidsSet.size()), e);
        }
    }


    /**
     * method to add back all the vmids back for processing in the next iteration.
     * this is called if there was an exception from group component service.
     *
     * @param currSucess
     * @param currFailed
     */
    private void restoreGlobalSets(Set<Long> currSucess, Set<Long> currFailed) {
        synchronized (lock) {
            if (!currSucess.isEmpty()) {
                successOidsSet.forEach(vmId -> recordVmAction(vmId, false, currSucess, currFailed));
                successOidsSet = currSucess;
            }
            if (!currFailed.isEmpty()) {
                failedOidsSet.forEach(vmId -> recordVmAction(vmId, true, currSucess, currFailed));
                failedOidsSet = currFailed;
            }
        }
    }

    /**
     * This should ideally be called from a synchronous block to avoid concurrent mutation of sets
     * @param vmId
     * @param failed       if the action failed
     * @param successVMSet set of vmIds which had a successful action.
     * @param failedVMSet  set of vmIds which had a failed action.
     */
    private void recordVmAction(final Long vmId, final boolean failed, final Set<Long> successVMSet, final Set<Long> failedVMSet) {
        if (failed) {
            failedVMSet.add(vmId);
            successVMSet.remove(vmId);
        } else {
            failedVMSet.remove(vmId);
            successVMSet.add(vmId);
        }
    }

    /**
     * Handle successful actions
     *
     * @param actionView
     */
    public void handleActionSuccess(@Nonnull ActionView actionView) {
        handleActionUpdate(actionView, false);
    }

    /**
     * Handle failed actions
     *
     * @param actionView
     */
    public void handleActionFailure(@Nonnull ActionView actionView) {
        handleActionUpdate(actionView, true);
    }

    /**
     * Handle actions.
     * Checks if action is a valid action and add/remove from sets based on action type: failure/success
     *
     * @param action action received from actionStateUpdater
     * @param failed result of the action on the target
     */
    private void handleActionUpdate(@Nonnull ActionView action, boolean failed) {
        try {
            ActionDTO.Action actionDTO = action.getActionTranslation().getTranslationResultOrOriginal();
            ActionEntity actionEntity = ActionDTOUtil.getPrimaryEntity(actionDTO);
            if (!isValidAction(actionDTO, actionEntity)) {
                logger.debug("Only processing action on VMs running Environment type Cloud of action type Move or Resize");
                return;
            }
            recordVmActionWithLock(actionEntity.getId(), failed);
        } catch (UnsupportedActionException e) {
            logger.error("Could not update group with latest action.", e);
        }
    }

    /**
     * @param vmId
     * @param failed
     */
    @VisibleForTesting
    public void recordVmActionWithLock(final long vmId, final boolean failed) {
        synchronized (lock) {
            recordVmAction(vmId, failed, successOidsSet, failedOidsSet);
        }
    }

    /**
     * checks if action entity is VM and if action is of type MOVE or RESIZE and EnvironmentType is CLOUD
     * right now we are only interested in resize and move actions
     *
     * @param action
     * @param actionEntity
     * @return
     */
    private boolean isValidAction(@Nonnull ActionDTO.Action action, final ActionEntity actionEntity) {
        if (actionEntity.getType() == EntityType.VIRTUAL_MACHINE.getValue() &&
                actionEntity.getEnvironmentType() == EnvironmentType.CLOUD) {
            ActionType actionType = ActionDTOUtil.getActionInfoActionType(action);
            return actionType == ActionType.MOVE || actionType == ActionType.RESIZE;
        }
        return false;
    }

    /**
     * creates a FAILED_GROUP_CLOUD_VMS using group service.
     *
     * @return
     */
    private Group createFailedActionGroup() {
        final GroupDTO.GroupInfo.Builder requestBuilder = GroupDTO.GroupInfo.newBuilder()
                .setName(FAILED_GROUP_CLOUD_VMS)
                .setEntityType(EntityType.VIRTUAL_MACHINE.getValue());
        return groupServiceClient.createGroup(requestBuilder.build()).getGroup();
    }

    /**
     * retrieves FAILED_GROUP_CLOUD_VMS from group service if it exists
     *
     * @return
     */
    private Optional<Group> getFailedGroup() {
        Iterator<Group> groupResponse = groupServiceClient.getGroups(
            GetGroupsRequest.newBuilder()
                .addTypeFilter(Type.GROUP)
                .setPropertyFilters(
                    GroupPropertyFilterList.newBuilder()
                        .addPropertyFilters(
                            PropertyFilter.newBuilder()
                                .setStringFilter(
                                    StringFilter.newBuilder()
                                        .setStringPropertyRegex(FAILED_GROUP_CLOUD_VMS)
                                        .build())
                                .build())
                        .build())
                .build());

        // It's theoretically possible to have other groups with the same name, but there can only be one group with the same name + entity type.
        while (groupResponse.hasNext()) {
            Group group = groupResponse.next();
            if (group.getGroup().getEntityType() == EntityType.VIRTUAL_MACHINE.getValue()) {
                return Optional.of(group);
            }
        }
        return Optional.empty();
    }

    @VisibleForTesting
    public Set<Long> getSuccessOidsSet() {
        return successOidsSet;
    }

    @VisibleForTesting
    public Set<Long> getFailedOidsSet() {
        return failedOidsSet;
    }
}
