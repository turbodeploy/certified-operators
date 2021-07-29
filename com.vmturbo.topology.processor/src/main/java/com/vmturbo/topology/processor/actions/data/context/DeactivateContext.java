package com.vmturbo.topology.processor.actions.data.context;

import javax.annotation.Nonnull;

import com.vmturbo.auth.api.securestorage.SecureStorageClient;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.GroupAndPolicyRetriever;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 *  A class for collecting data needed for Deactivate action execution
 */
public class DeactivateContext extends AbstractActionExecutionContext {

    public DeactivateContext(@Nonnull final ExecuteActionRequest request,
                             @Nonnull final ActionDataManager dataManager,
                             @Nonnull final EntityStore entityStore,
                             @Nonnull final EntityRetriever entityRetriever,
                             @Nonnull final TargetStore targetStore,
                             @Nonnull final ProbeStore probeStore,
                             @Nonnull final GroupAndPolicyRetriever groupAndPolicyRetriever,
                             @Nonnull final SecureStorageClient secureStorageClient)
            throws ContextCreationException {
        super(request, dataManager, entityStore, entityRetriever, targetStore, probeStore,
            groupAndPolicyRetriever, secureStorageClient);
    }

    @Override
    protected ActionItemDTO.ActionType calculateSDKActionType(@Nonnull final ActionDTO.ActionType actionType) {
        if (targetEntityIsStorage()) {
            return ActionItemDTO.ActionType.DELETE;
        } else {
            return ActionItemDTO.ActionType.SUSPEND;
        }
    }

    /**
     * Get the primary entity ID for this action
     * Corresponds to the logic in
     *   {@link ActionDTOUtil#getPrimaryEntityId(Action) ActionDTOUtil.getPrimaryEntityId}.
     * In comparison to that utility method, because we know the type here we avoid the switch
     * statement and the corresponding possiblity of an {@link UnsupportedActionException} being
     * thrown.
     *
     * @return the ID of the primary entity for this action (the entity being acted upon)
     */
    @Override
    protected long getPrimaryEntityId() {
        return getDeactivateInfo().getTarget().getId();
    }

    /**
     * A convenience method for getting the Deactivate info.
     *
     * @return the Deactivate info associated with this action
     */
    private Deactivate getDeactivateInfo() {
        return getActionInfo().getDeactivate();
    }

    /**
     * Checks if the target entity for the Deactivate info is Storage.
     *
     * @return true if target entity is Storage, false otherwise
     */
    private boolean targetEntityIsStorage() {
        return getDeactivateInfo().hasTarget() &&
            getDeactivateInfo().getTarget().hasType() &&
            getDeactivateInfo().getTarget().getType() == EntityType.STORAGE_VALUE;
    }
}
