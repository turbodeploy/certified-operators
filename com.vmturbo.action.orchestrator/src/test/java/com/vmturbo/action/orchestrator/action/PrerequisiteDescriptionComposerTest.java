package com.vmturbo.action.orchestrator.action;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.Prerequisite;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.PrerequisiteType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests for composing action pre-requisite descriptions in {@link PrerequisiteDescriptionComposer}.
 */
public class PrerequisiteDescriptionComposerTest {

    /**
     * Test {@link PrerequisiteDescriptionComposer#composePrerequisiteDescription}.
     */
    @Test
    public void testComposePrerequisiteDescription() {
        ActionDTO.Action action = ActionDTO.Action.newBuilder().setId(0)
            .setInfo(ActionInfo.newBuilder().setMove(
                Move.newBuilder().setTarget(ActionEntity.newBuilder()
                    .setId(1).setType(EntityType.VIRTUAL_MACHINE.getNumber()))))
            .setDeprecatedImportance(0)
            .setExplanation(Explanation.getDefaultInstance())
            .addAllPrerequisite(Arrays.asList(
                Prerequisite.newBuilder().setPrerequisiteType(PrerequisiteType.ENA).build(),
                Prerequisite.newBuilder().setPrerequisiteType(PrerequisiteType.NVME).build(),
                Prerequisite.newBuilder().setPrerequisiteType(PrerequisiteType.ARCHITECTURE).build(),
                Prerequisite.newBuilder().setPrerequisiteType(PrerequisiteType.VIRTUALIZATION_TYPE).build(),
                Prerequisite.newBuilder().setPrerequisiteType(PrerequisiteType.LOCKS)
                    .setLocks("[Scope: vm1, name: vm-lock-1, notes: VM lock]").build(),
                Prerequisite.newBuilder().setPrerequisiteType(PrerequisiteType.CORE_QUOTAS)
                    .setRegionId(123).setQuotaName("test_quota_name").build(),
                Prerequisite.newBuilder().setPrerequisiteType(PrerequisiteType.SCALE_SET).build()))
            .build();

        assertEquals(new HashSet<>(Arrays.asList(
            "(^_^)~To unblock, enable ENA for {entity:1:displayName:Virtual Machine}. " +
                "Alternatively, you can exclude templates that require ENA",
            "(^_^)~To unblock, enable NVMe for {entity:1:displayName:Virtual Machine} and change instance " +
                "type in the AWS Console. Alternatively, you can exclude templates that require NVMe",
            "(^_^)~To unblock, enable 64-bit AMIs for {entity:1:displayName:Virtual Machine}. " +
                "Alternatively, you can exclude templates that require 64-bit AMIs",
            "(^_^)~To unblock, enable HVM AMIs for {entity:1:displayName:Virtual Machine}. " +
                "Alternatively, you can exclude templates that require HVM AMIs",
            "(^_^)~To execute action on {entity:1:displayName:Virtual Machine}, please remove these" +
                    " read-only locks: [Scope: vm1, name: vm-lock-1, notes: VM lock]",
            "(^_^)~Request a quota increase for test_quota_name in {entity:123:displayName:Region} to " +
                "allow resize of {entity:1:displayName:Virtual Machine}",
            "(^_^)~To execute action on {entity:1:displayName:Virtual Machine}, navigate to the Azure portal and adjust the scale set instance size")),
            new HashSet<>(PrerequisiteDescriptionComposer.composePrerequisiteDescription(action)));
    }

    /**
     * Test prerequisite description for Azure scaleset volume scale action.
     */
    @Test
    public void testComposeScaleSetVolumeScaleActionPrerequisiteDescription() {
        ActionDTO.Action action = ActionDTO.Action.newBuilder().setId(0)
                .setInfo(ActionInfo.newBuilder().setScale(
                        Scale.newBuilder().setTarget(ActionEntity.newBuilder()
                                .setId(1).setType(EntityType.VIRTUAL_VOLUME.getNumber()))))
                .setDeprecatedImportance(0)
                .setExplanation(Explanation.getDefaultInstance())
                .addAllPrerequisite(Arrays.asList(Prerequisite.newBuilder()
                        .setPrerequisiteType(PrerequisiteType.SCALE_SET).build()))
                .build();
        List<String> prerequisites = PrerequisiteDescriptionComposer.composePrerequisiteDescription(action);
        assertEquals(1, prerequisites.size());
        assertEquals("(^_^)~To execute action on {entity:1:displayName:Virtual Volume}, "
                + "navigate to the Azure portal and adjust at the scale set", prerequisites.get(0));
    }

}
