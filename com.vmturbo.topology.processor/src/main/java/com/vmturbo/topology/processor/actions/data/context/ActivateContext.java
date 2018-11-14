package com.vmturbo.topology.processor.actions.data.context;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.topology.processor.actions.data.ActionDataManager;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.entity.EntityStore;

/**
 *  A class for collecting data needed for Activate action execution
 */
public class ActivateContext extends AbstractActionExecutionContext {

    public ActivateContext(@Nonnull final ExecuteActionRequest request,
                           @Nonnull final ActionDataManager dataManager,
                           @Nonnull final EntityStore entityStore,
                           @Nonnull final EntityRetriever entityRetriever) {
        super(request, dataManager, entityStore, entityRetriever);
    }

    /**
     * Get the SDK (probe-facing) type of the over-arching action being executed
     *
     * @return the SDK (probe-facing) type of the over-arching action being executed
     */
    @Override
    public ActionType getSDKActionType() {
        return ActionType.START;
    }

    /**
     * Get the type of the over-arching action being executed
     *
     * @return the type of the over-arching action being executed
     */
    @Override
    public ActionDTO.ActionType getActionType() {
        return ActionDTO.ActionType.ACTIVATE;
    }

    /**
     * Get the primary entity ID for this action
     * Corresponds to the logic in
     *   {@link com.vmturbo.common.protobuf.ActionDTOUtil#getPrimaryEntityId(Action) ActionDTOUtil.getPrimaryEntityId}.
     * In comparison to that utility method, because we know the type here we avoid the switch
     * statement and the corresponding possiblity of an {@link UnsupportedActionException} being
     * thrown.
     *
     * @return the ID of the primary entity for this action (the entity being acted upon)
     */
    @Override
    protected long getPrimaryEntityId() {
        return getActionInfo().getActivate().getTarget().getId();
    }
}
