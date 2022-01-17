package com.vmturbo.mediation.actionscript.executor;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;

/**
 * Class holds util methods.
 */
public class ActionScriptExecutorUtils {

    /**
     * Private constructor for util class.
     */
    private ActionScriptExecutorUtils() {}

    /**
     * Replace ActionExecutionDTO#actionOid with the stable action ID.
     *
     * @param actionExecutionDTO action execution DTO
     * @return updated action execution DTO
     */
    public static ActionExecutionDTO replaceActionInstanceIdWithStableId(
            @Nonnull final ActionExecutionDTO actionExecutionDTO) {
        return ActionExecutionDTO.newBuilder(actionExecutionDTO)
                .setActionOid(actionExecutionDTO.getActionStableId())
                .build();
    }
}
