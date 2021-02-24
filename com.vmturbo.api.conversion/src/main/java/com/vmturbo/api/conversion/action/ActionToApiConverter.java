package com.vmturbo.api.conversion.action;

import java.util.Optional;

import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.notification.LogEntryApiDTO;
import com.vmturbo.api.enums.ActionMode;
import com.vmturbo.api.enums.ActionState;
import com.vmturbo.api.utils.DateTimeUtil;

/**
 * Class that converts the the internal action object to API object.
 */
public class ActionToApiConverter {

    /**
     * Creates action API object based on the an action {@code ActionInformationProvider} object.
     *
     * @param action the object that provides information for creating action.
     * @param useStableActionIdAsUuid if we should use stable OID as action UUID.
     * @param topologyContextId the topology context for which action is created.
     * @param isPlan if the action is part of a plan.
     * @return the created API object.
     */
    public ActionApiDTO convert(ActionInformationProvider action,
            boolean useStableActionIdAsUuid,
            long topologyContextId,
            boolean isPlan) {
        // Construct a response ActionApiDTO to return
        final ActionApiDTO actionApiDTO = new ActionApiDTO();
        if (useStableActionIdAsUuid && !isPlan) {
            actionApiDTO.setUuid(Long.toString(action.getRecommendationId()));
            actionApiDTO.setActionID(action.getRecommendationId());
        } else {
            actionApiDTO.setUuid(Long.toString(action.getUuid()));
            actionApiDTO.setActionID(action.getUuid());
        }
        // Populate the action OID
        actionApiDTO.setActionImpactID(action.getRecommendationId());
        // set ID of topology/market for which the action is generated
        actionApiDTO.setMarketID(topologyContextId);
        // actionMode is direct translation
        final Optional<ActionMode> actionMode = action.getActionMode();
        actionMode.ifPresent(actionApiDTO::setActionMode);

        // For plan action, set the state to successes, so it will not be selectable
        if (!isPlan) {
            action.getActionState()
                .ifPresent(actionApiDTO::setActionState);
        } else {
            // In classic all the plan actions have "Succeeded" state; in XL all the plan actions
            // have default state (ready). Set the state to "Succeeded" here to make it Not selectable
            // on plan UI.
            actionApiDTO.setActionState(ActionState.SUCCEEDED);
        }

        if (actionMode.isPresent()) {
            actionApiDTO.setDisplayName(actionMode.get().name());
        }

        // Set prerequisites for actionApiDTO if actionSpec has any pre-requisite description.
        if (!action.getPrerequisiteDescriptionList().isEmpty()) {
            actionApiDTO.setPrerequisites(action.getPrerequisiteDescriptionList());
        }

        // map the recommendation info
        LogEntryApiDTO risk = new LogEntryApiDTO();
        actionApiDTO.setImportance((float)0.0);
        risk.setImportance((float)0.0);
        // set the explanation string

        risk.setDescription(action.getRiskDescription());
        risk.setSubCategory(action.getCategory());
        risk.setSeverity(action.getSeverity());
        risk.setReasonCommodity("");
        risk.addAllReasonCommodities(action.getReasonCommodities());
        actionApiDTO.setRisk(risk);

        // The target definition
        actionApiDTO.setStats(action.getStats());

        // Action details has been set in AO
        actionApiDTO.setDetails(action.getDescription());

        // set the type
        actionApiDTO.setActionType(action.getActionType());

        // set the target for the action
        actionApiDTO.setTarget(action.getTarget());

        // set the current SE of action if present
        action.getCurrentEntity().ifPresent(actionApiDTO::setCurrentEntity);

        // set the new SE of action if present
        action.getNewEntity().ifPresent(actionApiDTO::setNewEntity);

        // sets the current, new, resize to value of the action
        action.getCurrentValue().ifPresent(actionApiDTO::setCurrentValue);
        action.getNewValue().ifPresent(actionApiDTO::setNewValue);

        action.getValueUnit().ifPresent(actionApiDTO::setValueUnits);

        action.getRelatedPolicy().ifPresent(actionApiDTO::setPolicy);

        // record the times for this action
        final String createTime = DateTimeUtil.toString(action.getCreationTime());
        actionApiDTO.setCreateTime(createTime);

        action.getUpdatingTime().map(DateTimeUtil::toString).ifPresent(actionApiDTO::setUpdateTime);

        // set accepting user
        action.getAcceptingUser().ifPresent(actionApiDTO::setUserName);

        // set the execution characteristics
        action.getActionExecutionCharacteristics()
            .ifPresent(actionApiDTO::setExecutionCharacteristics);

        // add the compound actions for the action
        actionApiDTO.addCompoundActions(action.getCompoundActions());

        return actionApiDTO;
    }
}
