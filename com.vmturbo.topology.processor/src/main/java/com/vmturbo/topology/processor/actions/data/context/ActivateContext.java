package com.vmturbo.topology.processor.actions.data.context;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 *  A class for collecting data needed for Activate action execution
 */
public class ActivateContext extends AbstractActionExecutionContext {

    public ActivateContext(@Nonnull final ExecuteActionRequest request,
                           @Nonnull final ActionDataManager dataManager,
                           @Nonnull final EntityStore entityStore,
                           @Nonnull final EntityRetriever entityRetriever,
                           @Nonnull final TargetStore targetStore,
                           @Nonnull final ProbeStore probeStore) {
        super(request, dataManager, entityStore, entityRetriever, targetStore, probeStore);
    }

    @Override
    protected ActionItemDTO.ActionType calculateSDKActionType(@Nonnull final ActionDTO.ActionType actionType) {
        if (targetEntityIsStorage()) {
            return ActionType.ADD_PROVIDER;
        } else {
            return ActionType.START;
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
        return getActivateInfo().getTarget().getId();
    }

    /**
     * A convenience method for getting the Activate info.
     *
     * @return the Activate info associated with this action
     */
    private Activate getActivateInfo() {
        return getActionInfo().getActivate();
    }

    /**
     * Checks if the target entity for the Activate info is Storage.
     *
     * @return true if target entity is Storage, false otherwise
     */
    private boolean targetEntityIsStorage() {
        return getActivateInfo().hasTarget() &&
            getActivateInfo().getTarget().hasType() &&
            getActivateInfo().getTarget().getType() == EntityType.STORAGE_VALUE;
    }
}
