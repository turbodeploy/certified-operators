package com.vmturbo.topology.processor.group.settings;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.MaxUtilizationLevel;
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
    private static final long groupId = 123L;
    private static final GroupDTO.Grouping storageGroup = GroupDTO.Grouping.newBuilder()
        .setId(groupId)
        .addExpectedTypes(GroupDTO.MemberType.newBuilder().setEntity(EntityType.STORAGE.getValue()))
        .setDefinition(GroupDTO.GroupDefinition.newBuilder().setDisplayName("Storage Group"))
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
    public void testResolveGroupOverridesWithGroupMaxUtil() {
        Map<Long, GroupDTO.Grouping> groupsById = new HashMap<>();
        groupsById.put(groupId, storageGroup);
        List<ScenarioChange> changes = Lists.newArrayList(ScenarioChange.newBuilder()
            .setPlanChanges(PlanChanges.newBuilder()
                .setMaxUtilizationLevel(MaxUtilizationLevel.newBuilder()
                    .setGroupOid(groupId)
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
}
