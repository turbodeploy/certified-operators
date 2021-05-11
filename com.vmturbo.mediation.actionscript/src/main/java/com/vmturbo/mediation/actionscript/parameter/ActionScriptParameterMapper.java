package com.vmturbo.mediation.actionscript.parameter;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.actionscript.exception.ParameterMappingException;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.Parameter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * Maps values from an ActionExecutionDTO to action script parameters.
 */
public class ActionScriptParameterMapper {

    /**
     * This class only has static methods.
     */
    private ActionScriptParameterMapper() {

    }

    // Map containing, for each defined parameter, a function to extract the value for that parameter
    // from an ActionExecutionDTO.
    private static final Map<ActionScriptParameterDefinition, Function<ActionExecutionDTO, Optional<String>>>
        PARAMETER_VALUE_GETTER_MAP = createParamNameToValueGetters();

    /**
     * Try to map a value to each of the supplied parameters, from the supplied ActionExecutionDTO.
     *
     * <p>Values will be mapped to a parameter if the corresponding data is available. If the data is
     * not available or not applicable for a particular action, the corresponding variable is not
     * injected.</p>
     *
     * @param action an {@link ActionExecutionDTO} containing information about the action being
     *               executed, used to retrieve values for the parameters
     * @param params a list of names of script parameters that needs to have values mapped to them
     * @return a set of {@link ActionScriptParameter ActionScriptParameters} containing all the
     *         parameters that were successfully mapped, including the mapped values
     * @throws IllegalArgumentException if one of the parameters is not recognized
     * @throws ParameterMappingException if one of the parameters is required but no value can be
     *                                   mapped
     */
    public static Set<ActionScriptParameter> mapParameterValues(@Nonnull ActionExecutionDTO action,
                                                                @Nonnull List<Parameter> params) {
        final Set<ActionScriptParameter> paramsWithValues = new HashSet<>();
        // Iterate through the list of ActionScript parameters passed in to be mapped
        for (Parameter parameter : params) {
            // Try to map a value to this parameter.
            // If the parameter is not recognized, an IllegalArgumentException will be thrown.
            // If the parameter is required but no value can be mapped, a ParameterMappingException
            // will be thrown.
            mapParameterValue(parameter, action)
                // If the mapping function returned a value, include it in the parameters list
                .ifPresent(value -> paramsWithValues.add(value));
        }
        return paramsWithValues;
    }

    /**
     * An ActionScript parameter, with a value set.
     * Used to populate an environment variable prior to executing an ActionScript.
     */
    public static class ActionScriptParameter {
        private final String name;
        private final String value;
        private final String description;

        /**
         * Creates an parameter ready to populate a environment variable.
         *
         * @param parameterDefinition which environment variable to populate.
         * @param value the value to set the environment variable to.
         */
        public ActionScriptParameter(@Nonnull final ActionScriptParameterDefinition parameterDefinition,
                                     @Nonnull String value) {
            this.name = parameterDefinition.name();
            this.description = parameterDefinition.getDescription();
            this.value = Objects.requireNonNull(value);
        }

        public String getName() {
            return name;
        }

        public String getValue() {
            return value;
        }

        public String getDescription() {
            return description;
        }

        @Override
        public String toString() {
            return "ActionScriptParameter{"
                + "name='" + name + '\''
                + ", value='" + value + '\''
                + ", description='" + description + '\''
                + '}';
        }
    }

    /**
     * Create a map from parameter definition to a function to extract the value for that parameter.
     *
     * @return a map containing, for each defined parameter, a function to extract the value
     *         for that parameter from an ActionExecutionDTO
     */
    private static Map<ActionScriptParameterDefinition, Function<ActionExecutionDTO, Optional<String>>> createParamNameToValueGetters() {
        final Map<ActionScriptParameterDefinition, Function<ActionExecutionDTO, Optional<String>>> typeToFunction =
            new EnumMap<>(ActionScriptParameterDefinition.class);

        typeToFunction.put(ActionScriptParameterDefinition.VMT_ACTION_INTERNAL,
            action -> Optional.of(action)
                .filter(ActionExecutionDTO::hasActionOid)
                .map(ActionExecutionDTO::getActionOid)
                .map(String::valueOf)
            );

        // display name of the action to be consistent with the rest of the *_NAME fields
        typeToFunction.put(ActionScriptParameterDefinition.VMT_ACTION_NAME,
            action -> action.getActionItemList().stream()
                .findFirst()
                .filter(ActionItemDTO::hasDescription)
                .map(ActionItemDTO::getDescription)
            );

        // Use the OID for the internal name. The customer needs to be able to use this ID to
        // find the entity in the API.
        typeToFunction.put(ActionScriptParameterDefinition.VMT_CURRENT_INTERNAL,
            action -> getCurrentSE(action)
                .map(EntityDTO::getTurbonomicInternalId)
                .map(String::valueOf));
        typeToFunction.put(ActionScriptParameterDefinition.VMT_CURRENT_NAME,
            action -> getCurrentSE(action).map(EntityDTO::getDisplayName));

        // Use the OID for the internal name. The customer needs to be able to use this ID to
        // find the entity in the API.
        typeToFunction.put(ActionScriptParameterDefinition.VMT_NEW_INTERNAL,
            action -> getNewSE(action)
                .map(EntityDTO::getTurbonomicInternalId)
                .map(String::valueOf));
        typeToFunction.put(ActionScriptParameterDefinition.VMT_NEW_NAME,
            action -> getNewSE(action).map(EntityDTO::getDisplayName));

        // Use the OID for the internal name. The customer needs to be able to use this ID to
        // find the entity in the API.
        typeToFunction.put(ActionScriptParameterDefinition.VMT_TARGET_INTERNAL,
            action -> getTargetSE(action)
                .map(EntityDTO::getTurbonomicInternalId)
                .map(String::valueOf));
        typeToFunction.put(ActionScriptParameterDefinition.VMT_TARGET_NAME,
            action -> getTargetSE(action).map(EntityDTO::getDisplayName));

        // This needs to be the 3rd party, external id that the discovering probe provided.
        typeToFunction.put(ActionScriptParameterDefinition.VMT_TARGET_UUID,
            action -> getTargetSE(action)
                .map(EntityDTO::getId));

        return Collections.unmodifiableMap(typeToFunction);
    }

    /**
     * Try to map a value to this parameter, from the supplied ActionExecutionDTO.
     *
     * @param parameter the name of a script parameter that needs a value mapped
     * @param actionExecutionDTO containing information about the action being executed,
     *                           used to retrieve values for the parameter
     * @return an {@link ActionScriptParameter} including the value for this parameter,
     *         or {@link Optional#empty()} if the parameter is not required and no value
     *         can be mapped
     * @throws IllegalArgumentException if the parameter is not recognized
     * @throws ParameterMappingException if the parameter is required but no value can
     *                                   be mapped
     */
    private static Optional<ActionScriptParameter> mapParameterValue(
            @Nonnull final Parameter parameter,
            @Nonnull final ActionExecutionDTO actionExecutionDTO) {
        // Retrieve the parameter definition matching the name of the parameter to be mapped.
        // This will throw an IllegalArgumentException if the parameter name is not recognized.
        final ActionScriptParameterDefinition actionScriptParameterDefinition =
            ActionScriptParameterDefinition.valueOf(parameter.getName());
        // Use the parameter definition to lookup the function to be used to extract a value
        // for this parameter from the ActionExecutionDTO
        final Function<ActionExecutionDTO, Optional<String>> paramValueFunction =
            PARAMETER_VALUE_GETTER_MAP.get(actionScriptParameterDefinition);
        Optional<ActionScriptParameter> result = Optional.empty();
        // If there is a function to map a value for this parameter, apply it
        if (paramValueFunction != null) {
            result = paramValueFunction.apply(actionExecutionDTO)
                .map(value ->
                    new ActionScriptParameter(actionScriptParameterDefinition, value));
        }

        if (parameter.getMandatory() && !result.isPresent()) {
            // If no value was retrieved for a required parameter, throw a ParameterMappingException
            throw new ParameterMappingException("Couldn't find parameter setter for " + parameter);
        }

        // A non-mandatory parameter without a value can be omitted
        return result;
    }

    private static Optional<EntityDTO> getTargetSE(ActionExecutionDTO actionExecutionDTO) {
        if (actionExecutionDTO.getActionItemCount() > 0 && actionExecutionDTO.getActionItem(0).hasTargetSE()) {
            return Optional.of(actionExecutionDTO.getActionItem(0).getTargetSE());
        }
        return Optional.empty();
    }

    private static Optional<EntityDTO> getCurrentSE(ActionExecutionDTO actionExecutionDTO) {
        if (actionExecutionDTO.getActionItemCount() > 0 && actionExecutionDTO.getActionItem(0).hasCurrentSE()) {
            return Optional.of(actionExecutionDTO.getActionItem(0).getCurrentSE());
        }
        return Optional.empty();
    }

    private static Optional<EntityDTO> getNewSE(ActionExecutionDTO actionExecutionDTO) {
        if (actionExecutionDTO.getActionItemCount() > 0 && actionExecutionDTO.getActionItem(0).hasNewSE()) {
            return Optional.of(actionExecutionDTO.getActionItem(0).getNewSE());
        }
        return Optional.empty();
    }

}
