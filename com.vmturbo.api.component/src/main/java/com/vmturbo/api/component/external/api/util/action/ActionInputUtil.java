package com.vmturbo.api.component.external.api.util.action;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor.ActionStatsQuery;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.action.ActionResourceImpactStatApiInputDTO;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.api.enums.EntityType;

/**
 * Common functionality that has to do with action input.
 *
 * <p>This utility class provides the following functionality
 * - convert ActionResourceImpactStatApiInputDTO to ActionApiInputDTO
 * - create action resource identifier set with key pattern `statName_targetEntityType_actionType`
 *   from given ActionResourceImpactStatApiInputDTO
 * </p>
 */
public class ActionInputUtil {

    /**
     * Delimiter for action input resource impact. This is used in {@link ActionStatsMapper}.
     */
    public static final String ACTION_INPUT_RESOURCE_IMPACT_DELIMITER = "_";

    private ActionInputUtil() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Convert ActionResourceImpactStatApiInputDTO to ActionApiInputDTO.
     * This method is needed to operate on dependencies of AO and API query building for action
     * stats retrieval on ActionApiInputDTO.
     *
     * <p>TODO:: In future can be eliminated by updating
     * {@link ActionStatsQuery} to depend on {@link com.vmturbo.api.dto.action.BaseActionApiInputDTO}
     * instead of dependency on {@link ActionApiInputDTO}</p>
     *
     * @param inputDTO {@link ActionResourceImpactStatApiInputDTO} resource impact stat api input.
     * @return {@link ActionApiInputDTO} action api input.
     */
    public static ActionApiInputDTO toActionApiInputDTO(final @Nonnull ActionResourceImpactStatApiInputDTO inputDTO) {
        // Set groupBy to create buckets of action around targetEntity and actionType, this improves
        // data bucketing and filtering.
        final List<String> groupBy = new ArrayList<>();
        groupBy.add("targetType");
        groupBy.add("actionTypes");

        final ActionApiInputDTO actionApiInputDTO = new ActionApiInputDTO();
        actionApiInputDTO.setActionStateList(inputDTO.getActionStateList());
        actionApiInputDTO.setActionModeList(inputDTO.getActionModeList());
        actionApiInputDTO.setRiskSeverityList(inputDTO.getRiskSeverityList());
        actionApiInputDTO.setRiskSubCategoryList(inputDTO.getRiskSubCategoryList());
        actionApiInputDTO.setGroupBy(groupBy);
        actionApiInputDTO.setEnvironmentType(inputDTO.getEnvironmentType());
        actionApiInputDTO.setCostType(inputDTO.getCostType());
        actionApiInputDTO.setDescriptionQuery(inputDTO.getDescriptionQuery());
        actionApiInputDTO.setRiskQuery(inputDTO.getRiskQuery());
        actionApiInputDTO.setExecutionCharacteristics(inputDTO.getExecutionCharacteristics());
        actionApiInputDTO.setSavingsAmountRange(inputDTO.getSavingsAmountRange());
        actionApiInputDTO.setHasSchedule(inputDTO.getHasSchedule());
        actionApiInputDTO.setHasPrerequisites(inputDTO.getHasPrerequisites());

        // Restrict action buckets to provided actionTypes to reduce computational data set.
        final List<ActionType> actionTypeList = new ArrayList<>();
        if (inputDTO.getActionResourceImpactStatList() != null) {
            inputDTO.getActionResourceImpactStatList().forEach(actionResourceImpactStat -> {
                actionTypeList.add(actionResourceImpactStat.getActionType());
            });
        }
        actionApiInputDTO.setActionTypeList(actionTypeList);
        return actionApiInputDTO;
    }

    /**
     * Compute action resource impact identifier for the given resource impact stat input.
     * The unique identifier set is used for grouping action resource impacts, the grouping uses
     * statName, targetEntityType and actionType.
     * Please refer to {@link ActionStatsMapper} for details
     *
     * @param inputDTO {@link ActionResourceImpactStatApiInputDTO} resource impact stat api input.
     * @return set of unique identifiers in format 'statName_targetEntityType_actionType';
     */
    public static Set<String> toActionResourceImpactIdentifierSet(final @Nonnull ActionResourceImpactStatApiInputDTO inputDTO) {
        ObjectMapper jsonMapper = new ObjectMapper();
        final Set<String> actionResourceImpactIdentifierSet = new HashSet<>();
        if (inputDTO.getActionResourceImpactStatList() != null) {
            inputDTO.getActionResourceImpactStatList().forEach(actionResourceImpactStat -> {
                String statName = null;
                try {
                    String commodityType = jsonMapper.writeValueAsString(actionResourceImpactStat.getCommodityType());
                    statName = jsonMapper.readValue(commodityType, String.class);
                } catch (JsonProcessingException e) {
                    throw new IllegalArgumentException("Error converting commodityType to JSON String");
                }
                EntityType targetEntityType = actionResourceImpactStat.getTargetEntityType();
                String actionType = actionResourceImpactStat.getActionType().toString();
                String identifier = statName + ACTION_INPUT_RESOURCE_IMPACT_DELIMITER + targetEntityType.toString() + ACTION_INPUT_RESOURCE_IMPACT_DELIMITER + actionType;
                actionResourceImpactIdentifierSet.add(identifier);
            });
        }
        return actionResourceImpactIdentifierSet;
    }
}
