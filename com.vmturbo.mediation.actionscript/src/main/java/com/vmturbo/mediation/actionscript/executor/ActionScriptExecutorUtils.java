package com.vmturbo.mediation.actionscript.executor;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * Class holds util methods.
 */
public class ActionScriptExecutorUtils {

    /**
     * The key to get the stable action id from the ContextData.
     */
    public static final String STABLE_ID_KEY = "STABLE_ID";

    /**
     * Private constructor for util class.
     */
    private ActionScriptExecutorUtils() {}

    /**
     * Replace ActionExecutionDTO#actionOid with the stable action ID from the context data.
     * If there is no stable id, then throws an exception.
     *
     * @param actionExecutionDTO action execution DTO
     * @return updated action execution DTO
     */
    public static ActionExecutionDTO replaceActionInstanceIdWithStableId(
            @Nonnull final ActionExecutionDTO actionExecutionDTO) {
        final Optional<String> actionStableId = getContextValue(
                getPrimaryAction(actionExecutionDTO), STABLE_ID_KEY);
        if (actionStableId.isPresent()) {
            return ActionExecutionDTO.newBuilder(actionExecutionDTO).setActionOid(
                    Long.parseLong(actionStableId.get())).build();
        } else {
            throw new IllegalArgumentException(
                    "There is no \"" + STABLE_ID_KEY + "\" in the action context data.");
        }
    }

    @Nonnull
    private static ActionItemDTO getPrimaryAction(@Nonnull ActionExecutionDTO actionExecutionDTO) {
        if (actionExecutionDTO.getActionItemCount() > 0) {
            return actionExecutionDTO.getActionItem(0);
        } else {
            throw new IllegalStateException(
                    "The action \"" + actionExecutionDTO + "\" is missing action items.");
        }
    }

    private static Optional<String> getContextValue(@Nonnull ActionItemDTO actionItemDTO,
            @Nonnull String key) {
        return actionItemDTO.getContextDataList()
                .stream()
                .filter(c -> key.equals(c.getContextKey()))
                .map(CommonDTO.ContextData::getContextValue)
                .findAny();
    }
}
