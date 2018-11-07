package com.vmturbo.topology.processor.actions;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapabilityElement;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO.ActionCapability;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.util.SdkActionPolicyBuilder;

/**
 * Tests converting of sdk to xl policies.
 */
public class SdkToProbeActionsConverterTest {

    /**
     * Tests converting of sdk action policy to xl action policy.
     */
    @Test
    public void testConvert() {
        final ActionPolicyDTO sdkActionPolicy = SdkActionPolicyBuilder.build(ActionCapability
                .SUPPORTED, EntityType.PHYSICAL_MACHINE, ActionType.CHANGE);
        final ProbeActionCapability xlAction =  SdkToProbeActionsConverter.convert(sdkActionPolicy);
        assertIsConvertedSdkPolicy(sdkActionPolicy, xlAction, ActionDTO.ActionType.MOVE);
    }

    /**
     * Tests converting of list of sdk action policies to xl action policies.
     */
    @Test
    public void testConvertAll() {
        final List<ActionPolicyDTO> sdkPolicies = ImmutableList.of(
                SdkActionPolicyBuilder.build(ActionCapability.SUPPORTED,
                        EntityType.PHYSICAL_MACHINE, ActionType.CHANGE),
                SdkActionPolicyBuilder.build(ActionCapability.NOT_SUPPORTED,
                        EntityType.VIRTUAL_MACHINE, ActionType.RIGHT_SIZE),
                SdkActionPolicyBuilder.build(ActionCapability.SUPPORTED,
                        EntityType.PHYSICAL_MACHINE, ActionType.SUSPEND)
        );
        final List<ProbeActionCapability> xlActionPolicies =
                SdkToProbeActionsConverter.convert(sdkPolicies);
        assertIsConvertedSdkPolicy(sdkPolicies.get(0), xlActionPolicies.get(0),
                ActionDTO.ActionType.MOVE);
        assertIsConvertedSdkPolicy(sdkPolicies.get(1), xlActionPolicies.get(1),
                ActionDTO.ActionType.RESIZE);
        assertIsConvertedSdkPolicy(sdkPolicies.get(2), xlActionPolicies.get(2),
                ActionDTO.ActionType.DEACTIVATE);
    }

    /**
     * Tests MOVE action conversion. It is expected to be converted to an XL move actions across
     * everything but storages.
     */
    @Test
    public void testConvertMoveAction() {
        final ActionPolicyDTO moveAction = SdkActionPolicyBuilder.build(ActionCapability.SUPPORTED,
                EntityType.PHYSICAL_MACHINE, ActionType.MOVE);
        final ActionCapabilityElement result =
                SdkToProbeActionsConverter.convert(moveAction).getCapabilityElement(0);
        Assert.assertEquals(ActionDTO.ActionType.MOVE, result.getActionType());
        Assert.assertFalse(
                result.getMove().getTargetEntityTypeList().contains(EntityType.STORAGE_VALUE));
        Assert.assertTrue(result.getMove()
                .getTargetEntityTypeList()
                .contains(EntityType.PHYSICAL_MACHINE_VALUE));
    }

    /**
     * Tests CHANGE action conversion. It is expected to be converted to an XL move actions across
     * storages.
     */
    @Test
    public void testConvertChangeAction() {
        final ActionPolicyDTO moveAction = SdkActionPolicyBuilder.build(ActionCapability.SUPPORTED,
                EntityType.PHYSICAL_MACHINE, ActionType.CHANGE);
        final ActionCapabilityElement result =
                SdkToProbeActionsConverter.convert(moveAction).getCapabilityElement(0);
        Assert.assertEquals(ActionDTO.ActionType.MOVE, result.getActionType());
        Assert.assertTrue(
                result.getMove().getTargetEntityTypeList().contains(EntityType.STORAGE_VALUE));
        Assert.assertFalse(result.getMove()
                .getTargetEntityTypeList()
                .contains(EntityType.PHYSICAL_MACHINE_VALUE));
    }

    /**
     * Checks that there are same converted action policies.
     *
     * @param sdkActionPolicy policy before converting
     * @param xlActionPolicy converted policy
     */
    private void assertIsConvertedSdkPolicy(ActionPolicyDTO sdkActionPolicy,
            ProbeActionCapability xlActionPolicy, ActionDTO.ActionType convertedType) {
        Assert.assertEquals(sdkActionPolicy.getEntityType().getNumber(), xlActionPolicy.getEntityType());
        Assert.assertEquals(ProbeActionCapability.ActionCapability.forNumber(
                sdkActionPolicy.getPolicyElement(0).getActionCapability().getNumber()),
                xlActionPolicy.getCapabilityElement(0).getActionCapability());
        Assert.assertEquals(convertedType , xlActionPolicy.getCapabilityElement(0).getActionType());
    }
}
