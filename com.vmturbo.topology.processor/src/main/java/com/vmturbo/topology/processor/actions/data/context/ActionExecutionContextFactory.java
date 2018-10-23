package com.vmturbo.topology.processor.actions.data.context;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.topology.processor.actions.ActionExecutionException;
import com.vmturbo.topology.processor.actions.data.ActionDataManager;
import com.vmturbo.topology.processor.entity.EntityStore;

public abstract class ActionExecutionContextFactory {

    /**
     * Create an {@link ActionExecutionContext} to collect additional data needed in order to
     * execute the provided action.
     *
     * @param request an {@link ExecuteActionRequest} containing an action to be executed
     * @param entityStore an {@link EntityStore}, used to fetch additional data about the entities
     *                    involved in the action
     * @return an {@link ActionExecutionContext} to collect additional data needed in order to
     *         execute the provided action.
     * @throws ActionExecutionException
     */
    @Nonnull
    public static ActionExecutionContext getActionExecutionContext(
            @Nonnull final ExecuteActionRequest request,
            @Nonnull final ActionDataManager dataManager,
            @Nonnull final EntityStore entityStore)
            throws ActionExecutionException {
        if( ! request.hasActionInfo()) {
            throw new IllegalArgumentException("Cannot execute action with no action info. "
                    + "Action request: " + request.toString());
        }
        ActionInfo actionInfo = request.getActionInfo();
        switch (actionInfo.getActionTypeCase()) {
            case MOVE:
                return new MoveContext(request, dataManager, entityStore);
            case RESIZE:
                return new ResizeContext(request, dataManager, entityStore);
            case ACTIVATE:
                return new ActivateContext(request, dataManager, entityStore);
            case DEACTIVATE:
                return  new DeactivateContext(request, dataManager, entityStore);
            case PROVISION:
                return new ProvisionContext(request, dataManager, entityStore);
            default:
                throw new IllegalArgumentException("Unsupported action type: " +
                        actionInfo.getActionTypeCase());
        }
    }
}
