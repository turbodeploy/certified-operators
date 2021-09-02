package com.vmturbo.api.component.external.api.util.action;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.api.dto.QueryInputApiDTO;
import com.vmturbo.api.dto.RangeInputApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.action.ActionExecutionCharacteristicInputApiDTO;
import com.vmturbo.api.dto.action.ActionResourceImpactStatApiInputDTO;
import com.vmturbo.api.dto.action.ActionResourceImpactStatInput;
import com.vmturbo.api.enums.ActionCostType;
import com.vmturbo.api.enums.ActionDisruptiveness;
import com.vmturbo.api.enums.ActionMode;
import com.vmturbo.api.enums.ActionReversibility;
import com.vmturbo.api.enums.ActionState;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.api.enums.CommodityType;
import com.vmturbo.api.enums.EntityType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.QueryType;

/**
 * Test class for common functionality around action input.
 */
public class ActionInputUtilTest {

    /**
     * Test conversion of ActionResourceImpactStatApiInputDTO to ActionApiInputDTO without any filters.
     */
    @Test
    public void testToActionApiInputDTOWithoutFilters() {
        final ActionResourceImpactStatApiInputDTO inputDTO = new ActionResourceImpactStatApiInputDTO();
        final List<ActionResourceImpactStatInput> actionResourceImpactStatList = new ArrayList<>();
        final ActionResourceImpactStatInput statInput = new ActionResourceImpactStatInput();
        statInput.setTargetEntityType(EntityType.VirtualMachine);
        statInput.setActionType(ActionType.RESIZE);
        statInput.setCommodityType(CommodityType.VMEM);
        actionResourceImpactStatList.add(statInput);
        inputDTO.setActionResourceImpactStatList(actionResourceImpactStatList);

        ActionApiInputDTO actionApiInputDTO = ActionInputUtil.toActionApiInputDTO(inputDTO);
        Assert.assertThat(actionApiInputDTO.getActionTypeList(), Matchers.containsInAnyOrder(ActionType.RESIZE));
        Assert.assertThat(actionApiInputDTO.getGroupBy(), Matchers.containsInAnyOrder("targetType", "actionTypes"));
    }

    /**
     * Test conversion of ActionResourceImpactStatApiInputDTO to ActionApiInputDTO with input filters.
     */
    @Test
    public void testToActionApiInputDTOWithFilters() {
        final ActionResourceImpactStatApiInputDTO inputDTO = new ActionResourceImpactStatApiInputDTO();
        inputDTO.setActionStateList(Arrays.asList(ActionState.READY, ActionState.ACCEPTED, ActionState.QUEUED, ActionState.IN_PROGRESS));
        inputDTO.setActionModeList(Arrays.asList(ActionMode.AUTOMATIC, ActionMode.MANUAL));
        inputDTO.setRiskSeverityList(ImmutableList.of("Critical", "MAJOR"));
        inputDTO.setRiskSubCategoryList(Arrays.asList("Performance Assurance", "Compliance"));
        inputDTO.setEnvironmentType(EnvironmentType.CLOUD);
        inputDTO.setCostType(ActionCostType.SAVING);

        final QueryInputApiDTO descriptionQueryInputApiDTO = new QueryInputApiDTO();
        descriptionQueryInputApiDTO.setQuery(".*((.*Standard_D32_v3.*)).*");
        descriptionQueryInputApiDTO.setType(QueryType.REGEX);
        inputDTO.setDescriptionQuery(descriptionQueryInputApiDTO);

        final QueryInputApiDTO riskQueryInputApiDTO = new QueryInputApiDTO();
        riskQueryInputApiDTO.setQuery("(\\bVCPU Congestion\\b)");
        riskQueryInputApiDTO.setType(QueryType.REGEX);
        inputDTO.setRiskQuery(riskQueryInputApiDTO);

        final ActionExecutionCharacteristicInputApiDTO executionCharacteristicInputApiDTO = new ActionExecutionCharacteristicInputApiDTO();
        executionCharacteristicInputApiDTO.setDisruptiveness(ActionDisruptiveness.DISRUPTIVE);
        executionCharacteristicInputApiDTO.setReversibility(ActionReversibility.REVERSIBLE);
        inputDTO.setExecutionCharacteristics(executionCharacteristicInputApiDTO);

        final RangeInputApiDTO rangeInputApiDTO = new RangeInputApiDTO();
        rangeInputApiDTO.setMaxValue(0.14863013698630137f);
        rangeInputApiDTO.setMinValue(0.13219178082191782f);
        inputDTO.setSavingsAmountRange(rangeInputApiDTO);

        inputDTO.setHasSchedule(true);
        inputDTO.setHasPrerequisites(true);

        final List<ActionResourceImpactStatInput> actionResourceImpactStatList = new ArrayList<>();
        final ActionResourceImpactStatInput statInput = new ActionResourceImpactStatInput();
        statInput.setTargetEntityType(EntityType.VirtualMachine);
        statInput.setActionType(ActionType.RESIZE);
        statInput.setCommodityType(CommodityType.VMEM);
        actionResourceImpactStatList.add(statInput);
        inputDTO.setActionResourceImpactStatList(actionResourceImpactStatList);

        ActionApiInputDTO actionApiInputDTO = ActionInputUtil.toActionApiInputDTO(inputDTO);
        Assert.assertThat(actionApiInputDTO.getActionTypeList(), Matchers.containsInAnyOrder(ActionType.RESIZE));
        Assert.assertThat(actionApiInputDTO.getGroupBy(), Matchers.containsInAnyOrder("targetType", "actionTypes"));
        Assert.assertEquals(ActionCostType.SAVING, actionApiInputDTO.getCostType());
        Assert.assertEquals(EnvironmentType.CLOUD, actionApiInputDTO.getEnvironmentType());
        Assert.assertThat(actionApiInputDTO.getActionStateList(), Matchers.containsInAnyOrder(ActionState.READY, ActionState.ACCEPTED, ActionState.QUEUED, ActionState.IN_PROGRESS));
        Assert.assertEquals(".*((.*Standard_D32_v3.*)).*", actionApiInputDTO.getDescriptionQuery().getQuery());
        Assert.assertEquals(QueryType.REGEX, actionApiInputDTO.getDescriptionQuery().getType());
        Assert.assertFalse(actionApiInputDTO.getDescriptionQuery().getCaseSensitive());
        Assert.assertEquals("(\\bVCPU Congestion\\b)", actionApiInputDTO.getRiskQuery().getQuery());
        Assert.assertEquals(QueryType.REGEX, actionApiInputDTO.getRiskQuery().getType());
        Assert.assertFalse(actionApiInputDTO.getRiskQuery().getCaseSensitive());
        Assert.assertEquals(0.13219178082191782f, actionApiInputDTO.getSavingsAmountRange().getMinValue(), 0);
        Assert.assertEquals(0.14863013698630137f, actionApiInputDTO.getSavingsAmountRange().getMaxValue(), 0);
        Assert.assertTrue(actionApiInputDTO.getHasSchedule());
        Assert.assertTrue(actionApiInputDTO.getHasPrerequisites());
        Assert.assertEquals(ActionDisruptiveness.DISRUPTIVE, actionApiInputDTO.getExecutionCharacteristics().getDisruptiveness());
        Assert.assertEquals(ActionReversibility.REVERSIBLE, actionApiInputDTO.getExecutionCharacteristics().getReversibility());
    }

    /**
     * Test conversion of ActionResourceImpactStatApiInputDTO to identifier set.
     */
    @Test
    public void testToActionResourceImpactIdentifierSet() {
        final ActionResourceImpactStatApiInputDTO inputDTO = new ActionResourceImpactStatApiInputDTO();
        final List<ActionResourceImpactStatInput> actionResourceImpactStatList = new ArrayList<>();
        final ActionResourceImpactStatInput statInput = new ActionResourceImpactStatInput();
        statInput.setTargetEntityType(EntityType.VirtualMachine);
        statInput.setActionType(ActionType.RESIZE);
        statInput.setCommodityType(CommodityType.VMEM);
        actionResourceImpactStatList.add(statInput);
        inputDTO.setActionResourceImpactStatList(actionResourceImpactStatList);

        final Set<String> result = ActionInputUtil.toActionResourceImpactIdentifierSet(inputDTO);
        Assert.assertTrue(result.contains("VMem_VirtualMachine_RESIZE"));
    }
}
