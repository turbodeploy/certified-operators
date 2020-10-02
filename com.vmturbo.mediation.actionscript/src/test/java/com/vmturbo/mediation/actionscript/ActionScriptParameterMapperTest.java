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
     * Test mapping parameters: VMT_TARGET_UUID, VMT_TARGET_INTERNAL, VMT_CURRENT_INTERNAL, and
     * VMT_NEW_INTERNAL.
     */
    @Test
    public void testMappingParameters() {
        // Create a representation of the action in this test
        final ActionExecutionDTO actionExecutionDTO = createSampleAction();
        // Create a list of parameters representing all the default parameters
        final List<Parameter> parameters = Arrays.asList(
            parameter(ActionScriptParameterDefinition.VMT_TARGET_UUID),
            parameter(ActionScriptParameterDefinition.VMT_TARGET_INTERNAL),
            parameter(ActionScriptParameterDefinition.VMT_CURRENT_INTERNAL),
            parameter(ActionScriptParameterDefinition.VMT_NEW_INTERNAL)
        );
        // Test the mapping function
        final Map<String, ActionScriptParameter> actionScriptParameters =
            ActionScriptParameterMapper.mapParameterValues(actionExecutionDTO, parameters).stream()
                .collect(Collectors.toMap(ActionScriptParameter::getName, Function.identity()));
        // Validate the results
        Assert.assertEquals(4, actionScriptParameters.size());
        checkParameter(ActionScriptParameterDefinition.VMT_TARGET_UUID.name(),
            actionExecutionDTO.getActionItem(0).getTargetSE().getId(),
            actionScriptParameters);
        checkParameter(ActionScriptParameterDefinition.VMT_TARGET_INTERNAL.name(),
            actionExecutionDTO.getActionItem(0).getTargetSE().getTurbonomicInternalId(),
            actionScriptParameters);
        checkParameter(ActionScriptParameterDefinition.VMT_CURRENT_INTERNAL.name(),
            actionExecutionDTO.getActionItem(0).getCurrentSE().getTurbonomicInternalId(),
            actionScriptParameters);
        checkParameter(ActionScriptParameterDefinition.VMT_NEW_INTERNAL.name(),
            actionExecutionDTO.getActionItem(0).getNewSE().getTurbonomicInternalId(),
            actionScriptParameters);
    }

    private static Parameter parameter(ActionScriptParameterDefinition definition) {
        return Parameter.newBuilder()
            .setName(definition.name())
            .setType(ActionScriptDiscovery.WORKFLOW_PARAMETER_TYPE)
            .build();
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
        // Validate the results
        // There are currently two parameters whose values cannot be mapped (VMT_ACTION_INTERNAL and
        // VMT_ACTION_NAME). Therefore, all parameters except two should be mapped.
        // TODO: Update this when the mapping support changes
        final int expectedParametersCount = ActionScriptParameterDefinition.values().length - 2;
        Assert.assertEquals(expectedParametersCount, actionScriptParameters.size());
        // Check that every parameter successfully mapped has a non-empty value
        actionScriptParameters.stream()
            .forEach(actionScriptParameter ->
                Assert.assertFalse(actionScriptParameter.getValue().isEmpty()));
    }

    /**
     * Test mapping a single parameter--VMT_ACTION_NAME, which has no mapping function defined.
     */
    @Test
    public void testFailedMappingOfMandatoryParameter() {
        // Create a representation of the action in this test
        final ActionExecutionDTO actionExecutionDTO = createSampleAction();
        // Create a list of parameters representing all the default parameters
        final List<Parameter> parameters = Arrays.asList(Parameter.newBuilder()
            // Choosing VMT_ACTION_NAME, because it has no mapping function defined
            .setName(ActionScriptParameterDefinition.VMT_ACTION_NAME.name())
            .setType(ActionScriptDiscovery.WORKFLOW_PARAMETER_TYPE)
            .setMandatory(true)
            .build());
        // Expect a ParameterMappingException to be thrown because the parameter cannot be mapped
        thrown.expect(ParameterMappingException.class);
        // Test the mapping function
        final Set<ActionScriptParameter> actionScriptParameters =
            ActionScriptParameterMapper.mapParameterValues(actionExecutionDTO, parameters);
    }

    /**
     * Test mapping a single parameter--VMT_ACTION_NAME, which has no mapping function defined.
     */
    @Test
    public void testFailedMappingOfUnrecognizedParameter() {
        // Create a representation of the action in this test
        final ActionExecutionDTO actionExecutionDTO = createSampleAction();
        // Create a list of parameters representing all the default parameters
        final List<Parameter> parameters = Arrays.asList(Parameter.newBuilder()
            // Choosing VMT_ACTION_NAME, because it has no mapping function defined
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

    private static ActionExecutionDTO createSampleAction() {
        return ActionExecutionDTO.newBuilder()
            .setActionType(ActionType.MOVE)
            .addActionItem(ActionItemDTO.newBuilder()
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
}
