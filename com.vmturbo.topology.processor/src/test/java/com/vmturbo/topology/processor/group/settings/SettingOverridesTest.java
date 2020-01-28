package com.vmturbo.topology.processor.group.settings;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.MaxUtilizationLevel;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.topology.TopologyEntityUtils;

public class SettingOverridesTest {

    private final GroupResolver groupResolver = mock(GroupResolver.class);
    private TopologyGraph<TopologyEntity> topologyGraph = mock(TopologyGraph.class);
    private static final long stGroupId = 123L;
    private static final long pmGroupId = 321L;
    private static final GroupDTO.Grouping storageGroup = GroupDTO.Grouping.newBuilder()
        .setId(stGroupId)
        .addExpectedTypes(GroupDTO.MemberType.newBuilder().setEntity(EntityType.STORAGE.getValue()))
        .setDefinition(GroupDTO.GroupDefinition.newBuilder().setDisplayName("Storage Group"))
        .build();
    private static final GroupDTO.Grouping hostGroup = GroupDTO.Grouping.newBuilder()
        .setId(pmGroupId)
        .addExpectedTypes(GroupDTO.MemberType.newBuilder().setEntity(EntityType.PHYSICAL_MACHINE.getValue()))
        .setDefinition(GroupDTO.GroupDefinition.newBuilder().setDisplayName("PM Group"))
        .build();

    private static final TopologyEntityDTO.Builder entity1 = TopologyEntityDTO
            .newBuilder()
            .setOid(111L)
            .setEntityType(EntityType.PHYSICAL_MACHINE.getValue());
    private static final TopologyEntityDTO.Builder entity2 = TopologyEntityDTO
            .newBuilder()
            .setOid(222L)
            .setEntityType(EntityType.PHYSICAL_MACHINE.getValue());
    private static final TopologyEntityDTO.Builder entity3 = TopologyEntityDTO
            .newBuilder()
            .setOid(333L)
            .setEntityType(EntityType.STORAGE.getValue());
    private static final TopologyEntity topologyEntity1 = TopologyEntityUtils.topologyEntity(entity1);
    private static final TopologyEntity topologyEntity2 = TopologyEntityUtils.topologyEntity(entity2);
    private static final TopologyEntity topologyEntity3 = TopologyEntityUtils.topologyEntity(entity3);
    private static final Set<Long> entities = ImmutableSet.of(333L);
    private static final Set<Long> pms = ImmutableSet.of(111L, 222L);
    private static final Set<Long> pm1Set = ImmutableSet.of(111L);

    @Test
    public void testResolveGroupOverridesWithGlobalMaxUtil() {
        List<ScenarioChange> changes = Lists.newArrayList(ScenarioChange.newBuilder()
                .setPlanChanges(PlanChanges.newBuilder()
                        .setMaxUtilizationLevel(MaxUtilizationLevel.newBuilder()
                                .setSelectedEntityType(EntityType.PHYSICAL_MACHINE.getValue())
                                .setPercentage(20)))
                .build());
        SettingOverrides settingOverrides = new SettingOverrides(changes);
        when(topologyGraph.entitiesOfType(EntityType.PHYSICAL_MACHINE.getValue()))
                .thenReturn(Stream.of(topologyEntity1, topologyEntity2));
        settingOverrides.resolveGroupOverrides(new HashMap<>(), groupResolver, topologyGraph);
        Assert.assertTrue(settingOverrides.overridesForEntity.size() == 2);
        Assert.assertTrue(settingOverrides.overridesForEntity.get(111L)
                .get(EntitySettingSpecs.CpuUtilization.getSettingName())
                .getNumericSettingValue().getValue() == 20);
        Assert.assertTrue(settingOverrides.overridesForEntity.get(222L)
            .get(EntitySettingSpecs.CpuUtilization.getSettingName())
            .getNumericSettingValue().getValue() == 20);
        Assert.assertTrue(settingOverrides.overridesForEntity.get(111L)
            .get(EntitySettingSpecs.MemoryUtilization.getSettingName())
            .getNumericSettingValue().getValue() == 20);
        Assert.assertTrue(settingOverrides.overridesForEntity.get(222L)
            .get(EntitySettingSpecs.MemoryUtilization.getSettingName())
            .getNumericSettingValue().getValue() == 20);
    }

    @Test

    public void testResolveGroupOverridesWithStorageGroupMaxUtil() {
        Map<Long, GroupDTO.Grouping> groupsById = new HashMap<Long, GroupDTO.Grouping>();
        groupsById.put(stGroupId, storageGroup);
        List<ScenarioChange> changes = Lists.newArrayList(ScenarioChange.newBuilder()
            .setPlanChanges(PlanChanges.newBuilder()
                .setMaxUtilizationLevel(MaxUtilizationLevel.newBuilder()
                    .setGroupOid(stGroupId)
                    .setSelectedEntityType(EntityType.STORAGE.getValue())
                    .setPercentage(10)))
            .build());
        SettingOverrides settingOverrides = new SettingOverrides(changes);
        when(topologyGraph.entitiesOfType(EntityType.STORAGE.getValue()))
            .thenReturn(Stream.of(topologyEntity3));
        when(groupResolver.resolve(storageGroup, topologyGraph)).thenReturn(entities);
        settingOverrides.resolveGroupOverrides(groupsById, groupResolver, topologyGraph);
        Assert.assertTrue(settingOverrides.overridesForEntity.size() == 1);
        Assert.assertTrue(settingOverrides.overridesForEntity.get(333L)
            .get(EntitySettingSpecs.StorageAmountUtilization.getSettingName())
            .getNumericSettingValue().getValue() == 10);
    }

    @Test
    public void testResolveGroupOverridesWithClusterMaxUtil() {
        Map<Long, GroupDTO.Grouping> groupsById = new HashMap<>();
        groupsById.put(pmGroupId, hostGroup);
        List<ScenarioChange> changes = Lists.newArrayList(ScenarioChange.newBuilder()
            .setPlanChanges(PlanChanges.newBuilder()
                .setMaxUtilizationLevel(MaxUtilizationLevel.newBuilder()
                    .setGroupOid(pmGroupId)
                    .setSelectedEntityType(EntityType.PHYSICAL_MACHINE.getValue())
                    .setPercentage(20)))
            .build());
        SettingOverrides settingOverrides = new SettingOverrides(changes);
        when(topologyGraph.entitiesOfType(EntityType.PHYSICAL_MACHINE.getValue()))
            .thenReturn(Stream.of(topologyEntity1, topologyEntity2));
        when(groupResolver.resolve(hostGroup, topologyGraph)).thenReturn(pms);
        settingOverrides.resolveGroupOverrides(groupsById, groupResolver, topologyGraph);
        Assert.assertTrue(settingOverrides.overridesForEntity.size() == 2);
        Assert.assertTrue(settingOverrides.overridesForEntity.get(111L)
            .get(EntitySettingSpecs.CpuUtilization.getSettingName())
            .getNumericSettingValue().getValue() == 20);
        Assert.assertTrue(settingOverrides.overridesForEntity.get(111L)
            .get(EntitySettingSpecs.MemoryUtilization.getSettingName())
            .getNumericSettingValue().getValue() == 20);
        Assert.assertTrue(settingOverrides.overridesForEntity.get(222L)
            .get(EntitySettingSpecs.CpuUtilization.getSettingName())
            .getNumericSettingValue().getValue() == 20);
        Assert.assertTrue(settingOverrides.overridesForEntity.get(222L)
            .get(EntitySettingSpecs.MemoryUtilization.getSettingName())
            .getNumericSettingValue().getValue() == 20);
    }

    /**
     * Create Setting Override with group ID, entityType (PM) and Setting (Provision -> Disabled).
     * Apply s.t override works on one host out of two and make sure setting
     * is available for relevant host.
     */
    @Test
    public void testResolveGroupOverridesForHostGroupSettingOverride() {
        Map<Long, GroupDTO.Grouping> groupsById = new HashMap<>();
        String disabled = "DISABLED";
        groupsById.put(pmGroupId, hostGroup);
        List<ScenarioChange> changes = Lists.newArrayList(ScenarioChange.newBuilder()
            .setSettingOverride(buildSettingOverrideStringValue(EntitySettingSpecs.Provision.getSettingName(), disabled)
                .setEntityType(EntityType.PHYSICAL_MACHINE.getValue())
                .setGroupOid(pmGroupId)
                .build())
            .build());
        SettingOverrides settingOverrides = new SettingOverrides(changes);
        when(topologyGraph.entitiesOfType(EntityType.PHYSICAL_MACHINE.getValue()))
                .thenReturn(Stream.of(topologyEntity1, topologyEntity2));

        // Only return one host with 111L and leave host with id 222L
        when(groupResolver.resolve(hostGroup, topologyGraph)).thenReturn(pm1Set);

        settingOverrides.resolveGroupOverrides(groupsById, groupResolver, topologyGraph);

        // Only host with id 111L has setting disabled.
        Assert.assertTrue(settingOverrides.overridesForEntity.size() == 1);

        // Only host with id 111L has setting disabled.
        Assert.assertTrue(settingOverrides.overridesForEntity.get(111L)
                .get(EntitySettingSpecs.Provision.getSettingName())
                .getStringSettingValue().getValue().equals(disabled));

        // Host with id 222L has no setting.
        Assert.assertFalse(settingOverrides.overridesForEntity.containsKey(222L));
    }

    @Nonnull
    private SettingOverride.Builder buildSettingOverrideStringValue(String name, String value) {
        return SettingOverride.newBuilder().setSetting(Setting.newBuilder()
                .setSettingSpecName(name)
                .setStringSettingValue(StringSettingValue.newBuilder().setValue(value)));
    }
}
