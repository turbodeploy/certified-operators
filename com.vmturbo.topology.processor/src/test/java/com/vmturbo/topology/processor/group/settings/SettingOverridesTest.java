package com.vmturbo.topology.processor.group.settings;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings.SettingToPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.ResolvedGroup;
import com.vmturbo.topology.processor.topology.TopologyEntityUtils;

public class SettingOverridesTest {

    private TopologyGraph<TopologyEntity> topologyGraph = mock(TopologyGraph.class);
    private static final long stGroupId = 123L;
    private static final long pmGroupId = 321L;
    private static final long containerGroupId = 456L;
    private static final long containerSpecGroupId = 789L;
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
    private static final GroupDTO.Grouping containerSpecGroup = GroupDTO.Grouping.newBuilder()
        .setId(containerSpecGroupId)
        .addExpectedTypes(GroupDTO.MemberType.newBuilder().setEntity(EntityType.CONTAINER_SPEC.getValue()))
        .setDefinition(GroupDTO.GroupDefinition.newBuilder().setDisplayName("Container Spec Group"))
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
            .setOid(888L)
            .setEntityType(EntityType.STORAGE.getValue());
    private static final TopologyEntityDTO.Builder entity4 = TopologyEntityDTO
            .newBuilder()
            .setOid(999L)
            .setEntityType(EntityType.STORAGE.getValue());

    private static final TopologyEntity topologyEntity1 = TopologyEntityUtils.topologyEntity(entity1);
    private static final TopologyEntity topologyEntity2 = TopologyEntityUtils.topologyEntity(entity2);
    private static final TopologyEntity topologyEntity3 = TopologyEntityUtils.topologyEntity(entity3);
    private static final TopologyEntity topologyEntity4 = TopologyEntityUtils.topologyEntity(entity4);
    private static final Set<Long> entities = ImmutableSet.of(333L);
    private static final Set<Long> pm1Set = ImmutableSet.of(111L);

    private EntitySettingsScopeEvaluator scopeEvaluator = new EntitySettingsScopeEvaluator(
        new TopologyGraph<TopologyEntity>(new Long2ObjectOpenHashMap<>(), Collections.emptyMap()));

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
        settingOverrides.resolveGroupOverrides(Collections.emptyMap(), scopeEvaluator);
        Assert.assertEquals(1, settingOverrides.overridesForEntityType.size());
        Assert.assertEquals(1, settingOverrides.overridesForGroup.size());
    }

    /**
     * Test that setting for groups are applied to the members of the group.
     */
    @Test
    public void testResolveSettingOverridesForGroupMembers() {
        Map<Long, GroupDTO.Grouping> groupsById = new HashMap<>();
        groupsById.put(stGroupId, storageGroup);
        ScenarioChange storageAmountUtilizationChange =
            createScenarioChange(EntityType.STORAGE.getValue(),
            EntitySettingSpecs.StorageAmountUtilization, 10, Optional.of(stGroupId));
        List<ScenarioChange> changes = Lists.newArrayList(storageAmountUtilizationChange);
        SettingOverrides settingOverrides = new SettingOverrides(changes);
        Map<Long, ResolvedGroup> groups = Collections.singletonMap(storageGroup.getId(),
            resolvedGroup(storageGroup, ApiEntityType.STORAGE, entities));
        settingOverrides.resolveGroupOverrides(groups, scopeEvaluator);
        Assert.assertEquals(1, settingOverrides.overridesForEntity.size());
        Assert.assertTrue(10 == settingOverrides.overridesForEntity.get(333L)
            .get(EntitySettingSpecs.StorageAmountUtilization.getSettingName())
            .getNumericSettingValue().getValue());
    }

    /**
     * Test that same setting for multiple groups is applied to the members in those groups.
     */
    @Test
    public void testResolveGroupOverridesSettingMultiGroup() {
        Map<Long, GroupDTO.Grouping> groupsById = new HashMap<>();
        String disabled = "DISABLED";
        groupsById.putAll(ImmutableMap.of(pmGroupId, hostGroup, stGroupId, storageGroup));
        List<ScenarioChange> changes = Lists.newArrayList(ScenarioChange.newBuilder()
            .setSettingOverride(buildSettingOverrideStringValue(ConfigurableActionSettings.Provision
                .getSettingName(), disabled)
                .setEntityType(EntityType.PHYSICAL_MACHINE.getValue())
                .setGroupOid(pmGroupId)
                .build())
            .build(),
            ScenarioChange.newBuilder()
            .setSettingOverride(buildSettingOverrideStringValue(ConfigurableActionSettings.Provision
                .getSettingName(), disabled)
                .setEntityType(EntityType.STORAGE.getValue())
                .setGroupOid(stGroupId)
                .build())
            .build());
        SettingOverrides settingOverrides = new SettingOverrides(changes);

        when(topologyGraph.entitiesOfType(EntityType.PHYSICAL_MACHINE.getValue()))
        .thenReturn(Stream.of(topologyEntity1, topologyEntity2));
        when(topologyGraph.entitiesOfType(EntityType.STORAGE.getValue()))
        .thenReturn(Stream.of(topologyEntity3, topologyEntity4));

        Map<Long, ResolvedGroup> groups = ImmutableMap.of(
            hostGroup.getId(), resolvedGroup(hostGroup, ApiEntityType.PHYSICAL_MACHINE, Sets.newHashSet(topologyEntity1.getOid(), topologyEntity2.getOid())),
            storageGroup.getId(), resolvedGroup(storageGroup, ApiEntityType.STORAGE, Sets.newHashSet(topologyEntity3.getOid(), topologyEntity4.getOid())));

        settingOverrides.resolveGroupOverrides(groups, scopeEvaluator);

        Assert.assertTrue(settingOverrides.overridesForEntity.size() == 4);

        Assert.assertTrue(settingOverrides.overridesForEntity.entrySet().stream()
            .allMatch(entry -> entry.getValue().get(ConfigurableActionSettings.Provision.getSettingName())
            .getStringSettingValue().getValue().equals(disabled)));
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
        Map<Long, ResolvedGroup> groups = ImmutableMap.of(
            hostGroup.getId(), resolvedGroup(hostGroup, ApiEntityType.STORAGE, pm1Set),
            storageGroup.getId(), resolvedGroup(storageGroup, ApiEntityType.STORAGE, pm1Set));

        settingOverrides.resolveGroupOverrides(groups, scopeEvaluator);
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
            .setSettingOverride(buildSettingOverrideStringValue(ConfigurableActionSettings.Provision.getSettingName(), disabled)
                .setEntityType(EntityType.PHYSICAL_MACHINE.getValue())
                .setGroupOid(pmGroupId)
                .build())
            .build());
        SettingOverrides settingOverrides = new SettingOverrides(changes);
        when(topologyGraph.entitiesOfType(EntityType.PHYSICAL_MACHINE.getValue()))
                .thenReturn(Stream.of(topologyEntity1, topologyEntity2));

        // Only return one host with 111L and leave host with id 222L
        Map<Long, ResolvedGroup> groups = ImmutableMap.of(
            hostGroup.getId(), resolvedGroup(hostGroup, ApiEntityType.PHYSICAL_MACHINE, pm1Set));

        settingOverrides.resolveGroupOverrides(groups, scopeEvaluator);

        // Only host with id 111L has setting disabled.
        Assert.assertTrue(settingOverrides.overridesForEntity.size() == 1);

        // Only host with id 111L has setting disabled.
        Assert.assertTrue(settingOverrides.overridesForEntity.get(111L)
                .get(ConfigurableActionSettings.Provision.getSettingName())
                .getStringSettingValue().getValue().equals(disabled));

        // Host with id 222L has no setting.
        Assert.assertFalse(settingOverrides.overridesForEntity.containsKey(222L));
    }

    /**
     * Test resolveGroupOverrides for ContainerSpec settings.
     * <p/>
     * Settings changes on ContainerSpec group will be applied to corresponding containers aggregated
     * by the ContainerSpecs.
     */
    @Test
    public void testResolveGroupOverridesForContainerSpecGroup() {
        final TopologyEntity.Builder container1 = TopologyEntityUtils
            .topologyEntity(0, 0, 0, "Container1", EntityDTO.EntityType.CONTAINER);
        final TopologyEntity.Builder container2 = TopologyEntityUtils
            .topologyEntity(1, 0, 0, "Container2", EntityDTO.EntityType.CONTAINER);
        final TopologyEntity.Builder containerSpec = TopologyEntityUtils
            .topologyEntity(2, 0, 0, "ContainerSpec", EntityDTO.EntityType.CONTAINER_SPEC);

        TopologyEntityUtils.addConnectedEntity(container1, containerSpec.getOid(), ConnectionType.AGGREGATED_BY_CONNECTION);
        TopologyEntityUtils.addConnectedEntity(container2, containerSpec.getOid(), ConnectionType.AGGREGATED_BY_CONNECTION);

        final TopologyGraph<TopologyEntity> topologyGraph =
            TopologyEntityUtils.topologyGraphOf(container1, container2, containerSpec);
        scopeEvaluator = new EntitySettingsScopeEvaluator(topologyGraph);

        Map<Long, ResolvedGroup> groups = ImmutableMap.of(
            containerSpecGroup.getId(), resolvedGroup(containerSpecGroup, ApiEntityType.CONTAINER_SPEC,
                Sets.newHashSet(containerSpec.getOid())));

        final String recommend = "RECOMMEND";

        List<ScenarioChange> changes = Lists.newArrayList(ScenarioChange.newBuilder()
            .setSettingOverride(buildSettingOverrideStringValue(ConfigurableActionSettings.Resize.getSettingName(), recommend)
                .setEntityType(EntityType.CONTAINER_SPEC.getValue())
                .setGroupOid(containerSpecGroupId)
                .build())
            .build());
        SettingOverrides settingOverrides = new SettingOverrides(changes);
        settingOverrides.resolveGroupOverrides(groups, scopeEvaluator);

        // ContainerSpec settings override the settings of the corresponding containers aggregated by
        // the ContainerSpecs.
        Assert.assertEquals(3, settingOverrides.overridesForEntity.size());
        Assert.assertTrue(settingOverrides.overridesForEntity.containsKey(container1.getOid()));
        Assert.assertTrue(settingOverrides.overridesForEntity.containsKey(container2.getOid()));
        Assert.assertEquals(recommend, settingOverrides.overridesForEntity.get(container1.getOid())
            .get(ConfigurableActionSettings.Resize.getSettingName())
            .getStringSettingValue().getValue());
        Assert.assertEquals(recommend, settingOverrides.overridesForEntity.get(container2.getOid())
            .get(ConfigurableActionSettings.Resize.getSettingName())
            .getStringSettingValue().getValue());
    }

    /**
     * Tests settings override. User settings should be replaced by plan settings with same setting spec name.
     */
    @Test
    public void testOverrideSettings() {
        final Setting setting1 = Setting.newBuilder()
                        .setSettingSpecName(EntitySettingSpecs.CpuOverprovisionedPercentage
                                        .getSettingSpec().getName())
                        .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(600))
                        .build();
        final Map<String, SettingToPolicyId> userSettings = Collections
                        .singletonMap(setting1.getSettingSpecName(),
                                        SettingToPolicyId.newBuilder()
                                        .setSetting(setting1)
                                        .addSettingPolicyId(1111L)
                                        .build());

        ScenarioChange change = createScenarioChange(EntityType.PHYSICAL_MACHINE.getValue(),
            EntitySettingSpecs.CpuOverprovisionedPercentage, 1000, Optional.empty());
        SettingOverrides settingOverrides = new SettingOverrides(Collections.singletonList(change));
        final Collection<SettingToPolicyId> settings = settingOverrides
                        .overrideSettings(entity1, userSettings);
        SettingToPolicyId settingPolicy = settings.iterator().next();
        Setting resultSetting = settingPolicy.getSetting();
        Assert.assertEquals(1000, resultSetting.getNumericSettingValue().getValue(), 0.00001);
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

    private ResolvedGroup resolvedGroup(Grouping group, ApiEntityType type, Set<Long> members) {
        return new ResolvedGroup(group, Collections.singletonMap(type, members));
    }
}
