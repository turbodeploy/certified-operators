package com.vmturbo.topology.processor.actions;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ProbeActionPolicy;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO.ActionCapability;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.util.SdkActionPolicyBuilder;

/**
 * Tests converting of sdk to xl policies.
 */
public class SdkToProbeActionsConverterTest {

    private static final SdkToProbeActionsConverter SDK_TO_PROBE_ACTIONS_CONVERTER =
            new SdkToProbeActionsConverter();

    /**
     * Tests converting of sdk action policy to xl action policy.
     */
    @Test
    public void testConvert() {
        final ActionPolicyDTO sdkActionPolicy = SdkActionPolicyBuilder.build(ActionCapability
                .SUPPORTED, EntityType.PHYSICAL_MACHINE, ActionType.CHANGE);
        final ProbeActionPolicy xlAction =  SDK_TO_PROBE_ACTIONS_CONVERTER.convert(sdkActionPolicy);
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
        final List<ProbeActionPolicy> xlActionPolicies =
                SDK_TO_PROBE_ACTIONS_CONVERTER.convert(sdkPolicies);
        assertIsConvertedSdkPolicy(sdkPolicies.get(0), xlActionPolicies.get(0),
                ActionDTO.ActionType.MOVE);
        assertIsConvertedSdkPolicy(sdkPolicies.get(1), xlActionPolicies.get(1),
                ActionDTO.ActionType.RESIZE);
        assertIsConvertedSdkPolicy(sdkPolicies.get(2), xlActionPolicies.get(2),
                ActionDTO.ActionType.DEACTIVATE);
    }

    /**
     * Checks that there are same converted action policies.
     *
     * @param sdkActionPolicy policy before converting
     * @param xlActionPolicy converted policy
     */
    private void assertIsConvertedSdkPolicy(ActionPolicyDTO sdkActionPolicy,
            ProbeActionPolicy xlActionPolicy, ActionDTO.ActionType convertedType) {
        Assert.assertEquals(sdkActionPolicy.getEntityType().getNumber(), xlActionPolicy.getEntityType());
        Assert.assertEquals(ProbeActionPolicy.ActionCapability.forNumber(
                sdkActionPolicy.getPolicyElement(0).getActionCapability().getNumber()),
                xlActionPolicy.getPolicyElement(0).getActionCapability());
        Assert.assertEquals(convertedType , xlActionPolicy.getPolicyElement(0).getActionType());
    }
}
