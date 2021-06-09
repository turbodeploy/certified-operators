package com.vmturbo.mediation.actionscript;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.mediation.actionscript.exception.ParameterMappingException;
import com.vmturbo.mediation.actionscript.parameter.ActionScriptParameterDefinition;
import com.vmturbo.mediation.actionscript.parameter.ActionScriptParameterMapper;
import com.vmturbo.mediation.actionscript.parameter.ActionScriptParameterMapper.ActionScriptParameter;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.Parameter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests the {@link ActionScriptParameterMapper}, checking whether it can successfully
 * map values to parameters.
 */
public class ActionScriptParameterMapperTest {

    /**
     * Expect no exceptions thrown by default (can override in individual tests).
     */
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /**
     * Test mapping of all parameters defined in ActionScriptParameterDefinition.
     */
    @Test
    public void testMappingParameters() {
        // Create a representation of the action in this test
        final ActionExecutionDTO actionExecutionDTO = createSampleAction();
        // Create a list of parameters representing all the parameters
        final List<Parameter> parameters = Arrays.stream(ActionScriptParameterDefinition.values())
            .map(parameterDefinition -> parameterDefinition.name())
            .map(parameterName -> Parameter.newBuilder()
                .setName(parameterName)
                .setType(ActionScriptDiscovery.WORKFLOW_PARAMETER_TYPE)
                .setMandatory(false)
                .build())
            .collect(Collectors.toList());

        // Test the mapping function
        final Map<String, ActionScriptParameter> actionScriptParameters =
            ActionScriptParameterMapper.mapParameterValues(actionExecutionDTO, parameters).stream()
                .collect(Collectors.toMap(ActionScriptParameter::getName, Function.identity()));
        // Validate the results
        Assert.assertEquals(9, actionScriptParameters.size());

        checkParameter(ActionScriptParameterDefinition.VMT_ACTION_INTERNAL.name(),
            String.valueOf(actionExecutionDTO.getActionOid()),
            actionScriptParameters);
        checkParameter(ActionScriptParameterDefinition.VMT_ACTION_NAME.name(),
            actionExecutionDTO.getActionItem(0).getDescription(),
            actionScriptParameters);

        checkParameter(ActionScriptParameterDefinition.VMT_TARGET_UUID.name(),
            actionExecutionDTO.getActionItem(0).getTargetSE().getId(),
            actionScriptParameters);
        checkParameter(ActionScriptParameterDefinition.VMT_TARGET_INTERNAL.name(),
            actionExecutionDTO.getActionItem(0).getTargetSE().getTurbonomicInternalId(),
            actionScriptParameters);
        checkParameter(ActionScriptParameterDefinition.VMT_TARGET_NAME.name(),
            actionExecutionDTO.getActionItem(0).getTargetSE().getDisplayName(),
            actionScriptParameters);

        checkParameter(ActionScriptParameterDefinition.VMT_CURRENT_INTERNAL.name(),
            actionExecutionDTO.getActionItem(0).getCurrentSE().getTurbonomicInternalId(),
            actionScriptParameters);
        checkParameter(ActionScriptParameterDefinition.VMT_CURRENT_NAME.name(),
            actionExecutionDTO.getActionItem(0).getCurrentSE().getDisplayName(),
            actionScriptParameters);

        checkParameter(ActionScriptParameterDefinition.VMT_NEW_INTERNAL.name(),
            actionExecutionDTO.getActionItem(0).getNewSE().getTurbonomicInternalId(),
            actionScriptParameters);
        checkParameter(ActionScriptParameterDefinition.VMT_NEW_NAME.name(),
            actionExecutionDTO.getActionItem(0).getNewSE().getDisplayName(),
            actionScriptParameters);
    }

    private void checkParameter(String name, long expectedValue, Map<String, ActionScriptParameter> params) {
        checkParameter(name, String.valueOf(expectedValue), params);
    }

    private void checkParameter(String name, String expectedValue, Map<String, ActionScriptParameter> params) {
        ActionScriptParameter actionScriptParameter = params.get(name);
        // Check that the parameter has the right name
        Assert.assertEquals(name, actionScriptParameter.getName());
        // Check that the parameter has the right value
        Assert.assertEquals(expectedValue, actionScriptParameter.getValue());
    }

    /**
     * Test mapping all of the parameters defined in {@link ActionScriptParameterDefinition}.
     */
    @Test
    public void testMappingAllDefaultParameters() {
        // Create a representation of the action in this test
        final ActionExecutionDTO actionExecutionDTO = createSampleAction();
        // Create a list of parameters representing all the default parameters
        final List<Parameter> parameters = Arrays.stream(ActionScriptParameterDefinition.values())
            .map(parameterDefinition -> parameterDefinition.name())
            .map(parameterName -> Parameter.newBuilder()
                .setName(parameterName)
                .setType(ActionScriptDiscovery.WORKFLOW_PARAMETER_TYPE)
                .setMandatory(false)
                .build())
            .collect(Collectors.toList());
        // Test the mapping function
        final Set<ActionScriptParameter> actionScriptParameters =
            ActionScriptParameterMapper.mapParameterValues(actionExecutionDTO, parameters);
        final int expectedParametersCount = ActionScriptParameterDefinition.values().length;
        Assert.assertEquals(expectedParametersCount, actionScriptParameters.size());
        // Check that every parameter successfully mapped has a non-empty value
        actionScriptParameters.stream()
            .forEach(actionScriptParameter ->
                Assert.assertFalse(actionScriptParameter.getValue().isEmpty()));
    }

    /**
     * Test mapping a single parameter--VMT_CURRENT_NAME, that is not present in a resize action, but
     * the parameters definition says is mandatory.
     */
    @Test
    public void testFailedMappingOfMandatoryParameter() {
        // Create a representation of the action in this test
        final ActionExecutionDTO actionExecutionDTO = createSampleResizeAction();
        // Create a list of parameters representing all the default parameters
        final List<Parameter> parameters = Arrays.asList(Parameter.newBuilder()
            // Choosing VMT_ACTION_NAME, because it has no mapping function defined
            .setName(ActionScriptParameterDefinition.VMT_CURRENT_NAME.name())
            .setType(ActionScriptDiscovery.WORKFLOW_PARAMETER_TYPE)
            .setMandatory(true)
            .build());
        // Expect a ParameterMappingException to be thrown because the parameter cannot be mapped
        thrown.expect(ParameterMappingException.class);
        // Test the mapping function
        ActionScriptParameterMapper.mapParameterValues(actionExecutionDTO, parameters);
    }

    /**
     * Test mapping a parameter 'someRandomParameterName', which we never supported and never will.
     */
    @Test
    public void testFailedMappingOfUnrecognizedParameter() {
        // Create a representation of the action in this test
        final ActionExecutionDTO actionExecutionDTO = createSampleAction();
        // Create a list of parameters representing all the default parameters
        final List<Parameter> parameters = Arrays.asList(Parameter.newBuilder()
            // someRandomParameterName is not a valid parameter. valid parameters are in ActionScriptParameterDefinition
            .setName("someRandomParameterName")
            .setType(ActionScriptDiscovery.WORKFLOW_PARAMETER_TYPE)
            .setMandatory(true)
            .build());
        // Expect a IllegalArgumentException to be thrown because the parameter is not recognized
        thrown.expect(IllegalArgumentException.class);
        // Test the mapping function
        final Set<ActionScriptParameter> actionScriptParameters =
            ActionScriptParameterMapper.mapParameterValues(actionExecutionDTO, parameters);
    }

    /**
     * No action item should still be able to send the action id.
     */
    @Test
    public void testNoActionItem() {
        final ActionExecutionDTO emptyAction = ActionExecutionDTO.newBuilder()
            .setActionOid(123L)
            .setActionType(ActionType.MOVE)
            .build();
        final List<Parameter> parameters = Arrays.asList(ActionScriptParameterDefinition.values()).stream()
            .map(actionScriptParameterDefinition -> Parameter.newBuilder()
                // someRandomParameterName is not a valid parameter. valid parameters are in ActionScriptParameterDefinition
                .setName(actionScriptParameterDefinition.name())
                .setType(ActionScriptDiscovery.WORKFLOW_PARAMETER_TYPE)
                .setMandatory(false)
                .build())
            .collect(Collectors.toList());
        final Set<ActionScriptParameter> actionScriptParameters =
            ActionScriptParameterMapper.mapParameterValues(emptyAction, parameters);
        // action oid
        Assert.assertEquals(actionScriptParameters.toString(), 1, actionScriptParameters.size());
    }

    /**
     * Ensures that parameters that could not be found are not returned by
     * ActionScriptParameterMapper.mapParameterValues. Previously, this method would incorrectly
     * return invalid values like '0' for VMT_CURRENT_INTERNAL when the action did not have a
     * current entity.
     */
    @Test
    public void testNotPresentMapsToOptional() {
        final ActionExecutionDTO emptyAction = ActionExecutionDTO.newBuilder()
            .setActionType(ActionType.MOVE)
            .addActionItem(ActionItemDTO.newBuilder()
                .setUuid("emptyAction") // protobuf definition requires uuid
                .setTargetSE(EntityDTO.newBuilder() // protobuf definition requires targetSE
                    .setEntityType(EntityType.VIRTUAL_MACHINE)
                    .setId("vm12")
                    .setDisplayName("Restless VM"))
                .setActionType(ActionType.MOVE))
            .build();
        final List<Parameter> parameters = Arrays.asList(ActionScriptParameterDefinition.values()).stream()
            .map(actionScriptParameterDefinition -> Parameter.newBuilder()
                // someRandomParameterName is not a valid parameter. valid parameters are in ActionScriptParameterDefinition
                .setName(actionScriptParameterDefinition.name())
                .setType(ActionScriptDiscovery.WORKFLOW_PARAMETER_TYPE)
                .setMandatory(false)
                .build())
            .collect(Collectors.toList());
        final Set<ActionScriptParameter> actionScriptParameters =
            ActionScriptParameterMapper.mapParameterValues(emptyAction, parameters);
        // target name, target internal id, and action uuid are the only available parameters
        Assert.assertEquals(actionScriptParameters.toString(), 3, actionScriptParameters.size());
    }

    private static ActionExecutionDTO createSampleAction() {
        return ActionExecutionDTO.newBuilder()
            .setActionType(ActionType.MOVE)
            .setActionOid(123L)
            .addActionItem(ActionItemDTO.newBuilder()
                .setDescription("Move vm12 from p202 to pm205")
                .setUuid("UnnecessaryMove321")
                .setActionType(ActionType.MOVE)
                .setTargetSE(EntityDTO.newBuilder()
                    .setEntityType(EntityType.VIRTUAL_MACHINE)
                    .setId("vm12")
                    .setDisplayName("Restless VM"))
                .setCurrentSE(EntityDTO.newBuilder()
                    .setEntityType(EntityType.PHYSICAL_MACHINE)
                    .setId("pm202")
                    .setDisplayName("Perfectly Good PM"))
                .setNewSE(EntityDTO.newBuilder()
                    .setEntityType(EntityType.PHYSICAL_MACHINE)
                    .setId("pm205")
                    .setDisplayName("PM With Greener Grass")))
            .build();
    }

    private static ActionExecutionDTO createSampleResizeAction() {
        return ActionExecutionDTO.newBuilder()
            .setActionType(ActionType.RESIZE)
            .addActionItem(ActionItemDTO.newBuilder()
                .setUuid("UnnecessaryMove321")
                .setDescription("Resize VM from 4GB to 8GB")
                .setActionType(ActionType.RESIZE)
                .setTargetSE(EntityDTO.newBuilder()
                    .setEntityType(EntityType.VIRTUAL_MACHINE)
                    .setId("vm12")
                    .setDisplayName("Restless VM")))
            .build();
    }
}
