package com.vmturbo.topology.processor.group.settings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import gnu.trove.set.TLongSet;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphCreator;
import com.vmturbo.topology.processor.group.ResolvedGroup;
import com.vmturbo.topology.processor.group.settings.EntitySettingsScopeEvaluator.ScopedSettings;
import com.vmturbo.topology.processor.topology.TopologyEntityUtils;

/**
 * Tests for {@link EntitySettingsScopeEvaluator}.
 */
public class EntitySettingsScopeEvaluatorTest {

    private EntitySettingsScopeEvaluator scopeEvaluator;

    private static final long CONTAINER_SPEC_ID_1 = 1L;
    private static final long CONTAINER_SPEC_ID_2 = 2L;
    private static final long CONTAINER_SPEC_ID_3 = 3L;

    private static final long CONTAINER_ID_1 = 11L;
    private static final long CONTAINER_ID_2 = 12L;
    private static final long CONTAINER_ID_3 = 13L;

    private static final long VM_ID_1 = 21L;
    private static final long STORAGE_ID_1 = 31L;

    private static final long GROUP_ID_1 = 101L;
    private static final long GROUP_ID_2 = 102L;

    private final TopologyEntity.Builder containerSpec1 = entity(CONTAINER_SPEC_ID_1, EntityType.CONTAINER_SPEC);
    private final TopologyEntity.Builder containerSpec2 = entity(CONTAINER_SPEC_ID_2, EntityType.CONTAINER_SPEC);
    private final TopologyEntity.Builder containerSpec3 = entity(CONTAINER_SPEC_ID_3, EntityType.CONTAINER_SPEC);

    private final TopologyEntity.Builder container1 = entityAggregators(CONTAINER_ID_1,
        EntityType.CONTAINER, containerSpec1);
    private final TopologyEntity.Builder container2 = entityAggregators(CONTAINER_ID_2,
        EntityType.CONTAINER, containerSpec2);
    private final TopologyEntity.Builder container3 = entityAggregators(CONTAINER_ID_3,
        EntityType.CONTAINER, containerSpec2);

    private final TopologyEntity.Builder vm1 = entity(VM_ID_1, EntityType.VIRTUAL_MACHINE);
    private final TopologyEntity.Builder storage1 = entityAggregators(STORAGE_ID_1, EntityType.STORAGE, vm1);

    /**
     * Set up the test environment.
     */
    @Before
    public void setup() {
        final Long2ObjectMap<TopologyEntity.Builder> topology =
            new Long2ObjectOpenHashMap<>(Stream.of(container1, container2, container3, containerSpec1,
                containerSpec2, containerSpec3, vm1, storage1)
            .collect(Collectors.toMap(TopologyEntity.Builder::getOid, Function.identity())));

        final TopologyGraph<TopologyEntity> topologyGraph =
            new TopologyGraphCreator<>(topology).build();
        scopeEvaluator = new EntitySettingsScopeEvaluator(topologyGraph);
    }

    /**
     * testEvaluateNoImplicitScope.
     */
    @Test
    public void testEvaluateNoImplicitScope() {
        final ResolvedGroup group = group(GROUP_ID_1, vm1);
        final SettingPolicy sp = settingPolicy(EntityType.VIRTUAL_MACHINE_VALUE,
            EntitySettingSpecs.PercentileAggressivenessVirtualMachine,
            group);

        final Collection<ScopedSettings> scopedSettings =
            scopeEvaluator.evaluateScopes(sp, resolvedGroups(group), settingsInPolicy(sp));
        assertEquals(1, scopedSettings.size());

        final ScopedSettings scopedSetting = scopedSettings.iterator().next();
        assertEquals(1, scopedSetting.settingsForScope.size());
        assertEquals(EntitySettingSpecs.PercentileAggressivenessVirtualMachine.getSettingName(),
            scopedSetting.settingsForScope.iterator().next().getSettingSpecName());
        assertScopeMembers(scopedSetting, VM_ID_1);
    }

    /**
     * testEvaluateWithImplicitScopeOneGroup.
     */
    @Test
    public void testEvaluateWithImplicitScopeOneGroup() {
        final ResolvedGroup group = group(GROUP_ID_1, containerSpec1);
        final SettingPolicy sp = settingPolicy(EntityType.CONTAINER_SPEC_VALUE,
            EntitySettingSpecs.ContainerSpecVcpuIncrement,
            group);

        final Collection<ScopedSettings> scopedSettings =
            scopeEvaluator.evaluateScopes(sp, resolvedGroups(group), settingsInPolicy(sp));
        assertEquals(1, scopedSettings.size());

        final ScopedSettings scopedSetting = scopedSettings.iterator().next();
        assertEquals(1, scopedSetting.settingsForScope.size());
        assertEquals(EntitySettingSpecs.ContainerSpecVcpuIncrement.getSettingName(),
            scopedSetting.settingsForScope.iterator().next().getSettingSpecName());
        assertScopeMembers(scopedSetting, CONTAINER_ID_1, CONTAINER_SPEC_ID_1);
    }

    /**
     * testEvaluateWithImplicitScopeMultipleAggregations.
     */
    @Test
    public void testEvaluateWithImplicitScopeMultipleAggregations() {
        final ResolvedGroup group = group(GROUP_ID_1, containerSpec2);
        final SettingPolicy sp = settingPolicy(EntityType.CONTAINER_SPEC_VALUE,
            EntitySettingSpecs.ContainerSpecVcpuIncrement,
            group);

        final Collection<ScopedSettings> scopedSettings =
            scopeEvaluator.evaluateScopes(sp, resolvedGroups(group), settingsInPolicy(sp));
        assertEquals(1, scopedSettings.size());

        final ScopedSettings scopedSetting = scopedSettings.iterator().next();
        assertEquals(1, scopedSetting.settingsForScope.size());
        assertEquals(EntitySettingSpecs.ContainerSpecVcpuIncrement.getSettingName(),
            scopedSetting.settingsForScope.iterator().next().getSettingSpecName());
        assertScopeMembers(scopedSetting, CONTAINER_ID_2, CONTAINER_ID_3, CONTAINER_SPEC_ID_2);
    }

    /**
     * testEvaluateWithImplicitMultipleGroupsInScope.
     */
    @Test
    public void testEvaluateWithImplicitMultipleGroupsInScope() {
        final ResolvedGroup group1 = group(GROUP_ID_1, containerSpec1);
        final ResolvedGroup group2 = group(GROUP_ID_2, containerSpec2, containerSpec3);
        final SettingPolicy sp = settingPolicy(EntityType.CONTAINER_SPEC_VALUE,
            EntitySettingSpecs.ContainerSpecVcpuIncrement,
            group1, group2);

        final Collection<ScopedSettings> scopedSettings =
            scopeEvaluator.evaluateScopes(sp, resolvedGroups(group1, group2), settingsInPolicy(sp));
        assertEquals(1, scopedSettings.size());

        final ScopedSettings scopedSetting = scopedSettings.iterator().next();
        assertEquals(1, scopedSetting.settingsForScope.size());
        assertEquals(EntitySettingSpecs.ContainerSpecVcpuIncrement.getSettingName(),
            scopedSetting.settingsForScope.iterator().next().getSettingSpecName());
        assertScopeMembers(scopedSetting, CONTAINER_ID_1, CONTAINER_ID_2, CONTAINER_ID_3,
            CONTAINER_SPEC_ID_1, CONTAINER_SPEC_ID_2, CONTAINER_SPEC_ID_3);
    }

    /**
     * testEvaluateMultipleSettingsSameScope.
     */
    @Test
    public void testEvaluateMultipleSettingsSameScope() {
        final ResolvedGroup group1 = group(GROUP_ID_1, containerSpec1);
        final ResolvedGroup group2 = group(GROUP_ID_2, containerSpec2, containerSpec3);
        final SettingPolicy sp = settingPolicy(EntityType.CONTAINER_SPEC_VALUE,
            Arrays.asList(EntitySettingSpecs.ContainerSpecVcpuIncrement,
                EntitySettingSpecs.ContainerSpecVmemIncrement),
            group1, group2);

        final Collection<ScopedSettings> scopedSettings =
            scopeEvaluator.evaluateScopes(sp, resolvedGroups(group1, group2), settingsInPolicy(sp));
        assertEquals(1, scopedSettings.size());

        final ScopedSettings scopedSetting = scopedSettings.iterator().next();
        assertEquals(2, scopedSetting.settingsForScope.size());
        assertScopeMembers(scopedSetting, CONTAINER_ID_1, CONTAINER_ID_2, CONTAINER_ID_3,
            CONTAINER_SPEC_ID_1, CONTAINER_SPEC_ID_2, CONTAINER_SPEC_ID_3);
    }

    /**
     * testEvaluateMultipleSettingsDifferentScopes.
     */
    @Test
    public void testEvaluateMultipleSettingsDifferentScopes() {
        final ResolvedGroup group1 = group(GROUP_ID_1, containerSpec1);
        final ResolvedGroup group2 = group(GROUP_ID_2, containerSpec2, containerSpec3);
        final SettingPolicy sp = settingPolicy(EntityType.CONTAINER_SPEC_VALUE,
            Arrays.asList(EntitySettingSpecs.ContainerSpecVcpuIncrement,
                EntitySettingSpecs.PercentileAggressivenessVirtualMachine),
            group1, group2);

        final Collection<ScopedSettings> scopedSettings =
            scopeEvaluator.evaluateScopes(sp, resolvedGroups(group1, group2), settingsInPolicy(sp));
        assertEquals(2, scopedSettings.size());
        final ScopedSettings first = settingFor(scopedSettings, EntitySettingSpecs.ContainerSpecVcpuIncrement);
        assertScopeMembers(first, CONTAINER_ID_1, CONTAINER_ID_2, CONTAINER_ID_3,
            CONTAINER_SPEC_ID_1, CONTAINER_SPEC_ID_2, CONTAINER_SPEC_ID_3);

        final ScopedSettings second = settingFor(scopedSettings,
            EntitySettingSpecs.PercentileAggressivenessVirtualMachine);
        assertScopeMembers(second, CONTAINER_SPEC_ID_1, CONTAINER_SPEC_ID_2, CONTAINER_SPEC_ID_3);
    }

    /**
     * testEvaluateGroupNoImplicitScope.
     */
    @Test
    public void testEvaluateGroupNoImplicitScope() {
        final ResolvedGroup group = group(GROUP_ID_1, vm1);

        final TLongSet scope = scopeEvaluator.evaluateScopeForGroup(group,
            EntitySettingSpecs.PercentileAggressivenessVirtualMachine.getSettingName(),
            EntityType.VIRTUAL_MACHINE_VALUE);
        assertScopeMembers(scope, VM_ID_1);
    }

    /**
     * testEvaluateGroupWithImplicitScope.
     */
    @Test
    public void testEvaluateGroupWithImplicitScope() {
        final ResolvedGroup group = group(GROUP_ID_1, containerSpec1);

        final TLongSet scope = scopeEvaluator.evaluateScopeForGroup(group,
            EntitySettingSpecs.ContainerSpecVcpuIncrement.getSettingName(),
            EntityType.CONTAINER_SPEC_VALUE);
        assertScopeMembers(scope, CONTAINER_SPEC_ID_1, CONTAINER_ID_1);
    }

    /**
     * testEvaluateGroupForSet.
     */
    @Test
    public void testEvaluateGroupForSet() {
        final ResolvedGroup group = group(GROUP_ID_1, vm1, containerSpec1);

        final TLongSet scope = scopeEvaluator.evaluateScopeForGroup(group,
            EntitySettingSpecs.ContainerSpecVcpuIncrement.getSettingName(),
            ImmutableSet.of(EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER_SPEC));
        assertScopeMembers(scope, VM_ID_1, CONTAINER_SPEC_ID_1, CONTAINER_ID_1);
    }

    private void assertScopeMembers(final ScopedSettings scope, final long... memberOids) {
        assertScopeMembers(scope.scope, memberOids);
    }

    private void assertScopeMembers(final TLongSet scope, final long... memberOids) {
        assertEquals(memberOids.length, scope.size());
        for (long member : memberOids) {
            assertTrue(scope.contains(member));
        }
    }

    private TopologyEntity.Builder entity(final long oid, final EntityType entityType) {
        return TopologyEntityUtils.topologyEntityBuilder(
            TopologyEntityDTO.newBuilder()
                .setEntityType(entityType.getNumber())
                .setOid(oid));
    }

    private TopologyEntity.Builder entityAggregators(final long oid,
                                                     final EntityType entityType,
                                                     final TopologyEntity.Builder... aggregators) {
        final TopologyEntity.Builder entity = entity(oid, entityType);
        for (TopologyEntity.Builder agg : aggregators) {
            entity.getEntityBuilder().addConnectedEntityList(ConnectedEntity.newBuilder()
                .setConnectedEntityId(agg.getOid())
                .setConnectedEntityType(agg.getEntityType())
                .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION));
            entity.addAggregatedEntity(agg);
        }
        return entity;
    }

    private SettingPolicy settingPolicy(final int entityType,
                                        @Nonnull final EntitySettingSpecs spec,
                                        final ResolvedGroup... scopes) {
        return settingPolicy(entityType, Collections.singleton(spec), scopes);
    }

    private SettingPolicy settingPolicy(final int entityType,
                                        Collection<EntitySettingSpecs> specs,
                                        final ResolvedGroup... scopes) {
        final List<Long> groupIds = new ArrayList<>();
        for (ResolvedGroup g : scopes) {
            groupIds.add(g.getGroup().getId());
        }

        final Collection<Setting> settings = specs.stream()
            .map(spec -> Setting.newBuilder()
                .setSettingSpecName(spec.getSettingName())
                .setBooleanSettingValue(BooleanSettingValue.newBuilder()
                    .setValue(spec.getSettingSpec().getBooleanSettingValueType().getDefault()))
                .build()).collect(Collectors.toList());

        return SettingPolicy.newBuilder()
            .setId(1234L)
            .setInfo(SettingPolicyInfo.newBuilder()
                    .setEntityType(entityType)
                    .setDisplayName("setting-policy")
                    .setEnabled(true)
                    .setScope(Scope.newBuilder().addAllGroups(groupIds))
                    .addAllSettings(settings)
            ).build();
    }

    private Collection<TopologyProcessorSetting<?>> settingsInPolicy(@Nonnull final SettingPolicy sp) {
        return sp.getInfo().getSettingsList().stream()
            .map(setting -> TopologyProcessorSettingsConverter.toTopologyProcessorSetting(
                Collections.singleton(setting)))
            .collect(Collectors.toList());
    }

    private Map<Long, ResolvedGroup> resolvedGroups(final ResolvedGroup... groups) {
        final Map<Long, ResolvedGroup> groupMap = new HashMap<>();
        for (ResolvedGroup g : groups) {
            groupMap.put(g.getGroup().getId(), g);
        }
        return groupMap;
    }

    private ResolvedGroup group(final long groupId, final TopologyEntity.Builder... members) {
        final Map<ApiEntityType, Set<Long>> entitiesByType = new HashMap<>();
        for (TopologyEntity.Builder entity : members) {
            entitiesByType.computeIfAbsent(
                ApiEntityType.fromType(entity.getEntityType()), (type) -> new HashSet<>())
                .add(entity.getOid());
        }

        return new ResolvedGroup(
            Grouping.newBuilder().setId(groupId).build(), entitiesByType);
    }

    @Nullable
    private ScopedSettings settingFor(@Nonnull final Collection<ScopedSettings> settings,
                                      @Nonnull final EntitySettingSpecs spec) {
        return settings.stream()
            .filter(setting -> setting.settingsForScope.stream()
                .filter(s -> s.getSettingSpecName().equals(spec.getSettingName()))
                .findAny()
                .isPresent())
            .findFirst()
            .orElse(null);
    }
}