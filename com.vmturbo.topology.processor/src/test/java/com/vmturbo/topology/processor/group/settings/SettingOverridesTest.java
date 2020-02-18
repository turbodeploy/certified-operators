package com.vmturbo.topology.processor.group.settings;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
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

    private static final TopologyEntity topologyEntity1 = TopologyEntityUtils.topologyEntity(entity1);
    private static final TopologyEntity topologyEntity2 = TopologyEntityUtils.topologyEntity(entity2);
    private static final Set<Long> entities = ImmutableSet.of(333L);
    private static final Set<Long> pm1Set = ImmutableSet.of(111L);

    /**
     * Test that we parse setting overrides correctly, by distinguishing which ones should be
     * applied to groups, and which ones to entire entity types.
     */
    @Test
    public void testResolveGroupOverridesForEntityTypesAndGroups() {
        Long groupId = 1L;
        ScenarioChange cpuUtilizationChange  = createScenarioChange(EntityType.PHYSICAL_MACHINE.getValue(),
            EntitySettingSpecs.CpuUtilization, 20, Optional.of(groupId));

        ScenarioChange memoryUtilizationChange = createScenarioChange(EntityType.PHYSICAL_MACHINE.getValue(),
            EntitySettingSpecs.MemoryUtilization, 20, Optional.ofNullable(null));
        List<ScenarioChange> changes = Lists.newArrayList(cpuUtilizationChange, memoryUtilizationChange);
        SettingOverrides settingOverrides = new SettingOverrides(changes);
        settingOverrides.resolveGroupOverrides(new HashMap<>(), groupResolver, topologyGraph);
        Assert.assertEquals(1, settingOverrides.overridesForEntityType.size());
        Assert.assertEquals(1, settingOverrides.overridesForGroup.size());
    }

    /**
     * Test that setting for groups are applied to the members of the group.
     */
    @Test
    public void testResolveSettingOverridesForGroupMembers() {
        Map<Long, GroupDTO.Grouping> groupsById = new HashMap<Long, GroupDTO.Grouping>();
        groupsById.put(stGroupId, storageGroup);
        ScenarioChange storageAmountUtilizationChange =
            createScenarioChange(EntityType.STORAGE.getValue(),
            EntitySettingSpecs.StorageAmountUtilization, 10, Optional.of(stGroupId));
        List<ScenarioChange> changes = Lists.newArrayList(storageAmountUtilizationChange);
        SettingOverrides settingOverrides = new SettingOverrides(changes);
        when(groupResolver.resolve(storageGroup, topologyGraph)).thenReturn(entities);
        settingOverrides.resolveGroupOverrides(groupsById, groupResolver, topologyGraph);
        Assert.assertEquals(1, settingOverrides.overridesForEntity.size());
        Assert.assertTrue(10 == settingOverrides.overridesForEntity.get(333L)
            .get(EntitySettingSpecs.StorageAmountUtilization.getSettingName())
            .getNumericSettingValue().getValue());
    }

    /**
     * Test that if an entity belongs to two groups that have setting overrides, the tie breaker
     * correctly chooses which setting to apply.
     */
    @Test
    public void testResolveGroupOverridesWithTieBreaker() {
        Map<Long, GroupDTO.Grouping> groupsById = new HashMap<>();
        groupsById.put(pmGroupId, hostGroup);
        groupsById.put(stGroupId, storageGroup);

        ScenarioChange groupAStorageSetting =  createScenarioChange(EntityType.STORAGE.getValue(),
            EntitySettingSpecs.StorageAmountUtilization, 40, Optional.of(stGroupId));
        ScenarioChange groupBStorageSetting =   createScenarioChange(EntityType.STORAGE.getValue(),
            EntitySettingSpecs.StorageAmountUtilization, 10, Optional.of(pmGroupId));
        SettingOverrides settingOverrides =
            new SettingOverrides(Arrays.asList(groupAStorageSetting, groupBStorageSetting));

        // Entities in pm1Set belongs to two different groups
        when(groupResolver.resolve(hostGroup, topologyGraph)).thenReturn(pm1Set);
        when(groupResolver.resolve(storageGroup, topologyGraph)).thenReturn(pm1Set);

        settingOverrides.resolveGroupOverrides(groupsById, groupResolver, topologyGraph);
        // tie breaker is set to choose the smallest value
        Assert.assertTrue(settingOverrides.overridesForEntity.get(111L)
            .get(EntitySettingSpecs.StorageAmountUtilization.getSettingName())
            .getNumericSettingValue().getValue() == 10);
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

    private ScenarioChange createScenarioChange(int entityType,
                                                EntitySettingSpecs entitySettingSpec,
                                                int percentage,
                                                Optional<Long> groupId) {
        SettingOverride.Builder settingOverrideBuilder = SettingOverride.newBuilder();
        groupId.ifPresent(settingOverrideBuilder::setGroupOid);
        return ScenarioChange
            .newBuilder()
            .setSettingOverride(settingOverrideBuilder
                .setEntityType(entityType)
                .setSetting(Setting.newBuilder()
                    .setSettingSpecName(entitySettingSpec.getSettingName())
                    .setNumericSettingValue(NumericSettingValue.newBuilder()
                        .setValue(percentage).build())
                    .build())
            )
            .build();
    }
    @Nonnull
    private SettingOverride.Builder buildSettingOverrideStringValue(String name, String value) {
        return SettingOverride.newBuilder().setSetting(Setting.newBuilder()
                .setSettingSpecName(name)
                .setStringSettingValue(StringSettingValue.newBuilder().setValue(value)));
    }
}
