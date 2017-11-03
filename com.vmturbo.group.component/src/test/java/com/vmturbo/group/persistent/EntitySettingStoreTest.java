package com.vmturbo.group.persistent;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.jooq.exception.DataAccessException;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.TopologySelection;
import com.vmturbo.group.persistent.EntitySettingStore.ContextSettingSnapshotCache;
import com.vmturbo.group.persistent.EntitySettingStore.ContextSettingSnapshotCacheFactory;
import com.vmturbo.group.persistent.EntitySettingStore.EntitySettingSnapshot;
import com.vmturbo.group.persistent.EntitySettingStore.EntitySettingSnapshotFactory;
import com.vmturbo.group.persistent.EntitySettingStore.NoSettingsForTopologyException;

public class EntitySettingStoreTest {

    private final long realtimeContext = 7L;

    private SettingStore settingStore = mock(SettingStore.class);

    private EntitySettingSnapshotFactory snapshotFactory = mock(EntitySettingSnapshotFactory.class);

    private ContextSettingSnapshotCacheFactory cacheFactory = mock(ContextSettingSnapshotCacheFactory.class);

    private EntitySettingStore entitySettingStore =
            new EntitySettingStore(realtimeContext, settingStore, snapshotFactory, cacheFactory);

    private final EntitySettingFilter settingsFilter = EntitySettingFilter.newBuilder()
            .addEntities(1)
            .build();

    private final long contextId = 10L;
    private final long topologyId = 11L;

    private EntitySettingSnapshot mockSnapshot = mock(EntitySettingSnapshot.class);
    private ContextSettingSnapshotCache mockSnapshotCache = mock(ContextSettingSnapshotCache.class);

    private final Setting trueSetting = Setting.newBuilder()
            .setSettingSpecName("name1")
            .setBooleanSettingValue(BooleanSettingValue.newBuilder()
                    .setValue(true))
            .build();

    private final Setting falseSetting = Setting.newBuilder(trueSetting)
            .setBooleanSettingValue(BooleanSettingValue.newBuilder()
                    .setValue(true))
            .build();

    private final SettingPolicy settingPolicyWithFalseSetting = SettingPolicy.newBuilder()
            .setId(7L)
            .setSettingPolicyType(Type.DEFAULT)
            .setInfo(SettingPolicyInfo.newBuilder()
                    .addSettings(falseSetting))
            .build();

    @Before
    public void setup() {
        when(mockSnapshotCache.getSnapshot(anyLong())).thenReturn(Optional.empty());
        when(mockSnapshotCache.addSnapshot(anyLong(), any())).thenReturn(Optional.empty());
        when(snapshotFactory.createSnapshot(any(), any())).thenReturn(mockSnapshot);
        when(cacheFactory.newSnapshotCache()).thenReturn(mockSnapshotCache);
        when(settingStore.getSettingPolicies(eq(SettingPolicyFilter.newBuilder()
                .withType(Type.DEFAULT)
                .build())))
            .thenReturn(Stream.empty())
            .thenReturn(Stream.empty());
    }

    /**
     * Test to make sure that the non-test constructor doesn't generate any errors.
     */
    @Test
    public void testEntityStoreRealConstructor() {
        new EntitySettingStore(7L, settingStore);
    }

    @Test
    public void testStoreEntitySettings() {
        entitySettingStore.storeEntitySettings(contextId, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));

        verify(snapshotFactory).createSnapshot(any(), eq(Collections.emptyMap()));
        verify(cacheFactory).newSnapshotCache();
        verify(mockSnapshotCache).addSnapshot(eq(topologyId), eq(mockSnapshot));
    }

    @Test
    public void testDuplicateStoreEntitySettings() {
        entitySettingStore.storeEntitySettings(contextId, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));
        entitySettingStore.storeEntitySettings(contextId, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));

        verify(snapshotFactory, times(2)).createSnapshot(any(),
                eq(Collections.emptyMap()));
        verify(cacheFactory).newSnapshotCache();
        verify(mockSnapshotCache, times(2)).addSnapshot(eq(topologyId), eq(mockSnapshot));
    }

    @Test
    public void testStoreEntitySettingsDefaults() {
        final SettingPolicy settingPolicy = SettingPolicy.newBuilder()
            .setId(7L)
            .setInfo(SettingPolicyInfo.getDefaultInstance())
            .build();
        when(settingStore.getSettingPolicies(SettingPolicyFilter.newBuilder()
                .withType(Type.DEFAULT)
                .build()))
            .thenReturn(Stream.of(settingPolicy));

        entitySettingStore.storeEntitySettings(contextId, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));
        verify(snapshotFactory).createSnapshot(any(), eq(ImmutableMap.of(7L, settingPolicy)));
        verify(cacheFactory).newSnapshotCache();
        verify(mockSnapshotCache).addSnapshot(eq(topologyId), eq(mockSnapshot));
    }

    @Test(expected = DataAccessException.class)
    public void testStoreEntitySettingsDBException() {
        when(settingStore.getSettingPolicies(SettingPolicyFilter.newBuilder()
                .withType(Type.DEFAULT)
                .build()))
            .thenThrow(DataAccessException.class);
        entitySettingStore.storeEntitySettings(contextId, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));
    }

    @Test
    public void testStoreEntitySettingsMultipleTopologies() {
        entitySettingStore.storeEntitySettings(contextId, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));
        entitySettingStore.storeEntitySettings(contextId, topologyId + 1,
                Stream.of(EntitySettings.getDefaultInstance()));

        verify(snapshotFactory, times(2)).createSnapshot(any(),
                eq(Collections.emptyMap()));
        verify(cacheFactory).newSnapshotCache();
        verify(mockSnapshotCache).addSnapshot(eq(topologyId), eq(mockSnapshot));
        verify(mockSnapshotCache).addSnapshot(eq(topologyId + 1), eq(mockSnapshot));
    }

    @Test
    public void testGetEntitySettings() throws NoSettingsForTopologyException {
        entitySettingStore.storeEntitySettings(contextId, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));

        when(mockSnapshotCache.getSnapshot(eq(topologyId))).thenReturn(Optional.of(mockSnapshot));
        final Map<Long, Collection<Setting>> expectedMap =
                ImmutableMap.of(1L, Collections.emptyList());
        when(mockSnapshot.getFilteredSettings(eq(settingsFilter))).thenReturn(expectedMap);

        final Map<Long, Collection<Setting>> result =
            entitySettingStore.getEntitySettings(TopologySelection.newBuilder()
                .setTopologyContextId(contextId)
                .setTopologyId(topologyId)
                .build(), settingsFilter);

        verify(mockSnapshot).getFilteredSettings(eq(settingsFilter));
        assertEquals(expectedMap, result);
    }

    @Test
    public void testGetEntitySettingsIdButNoContext() throws NoSettingsForTopologyException {
        entitySettingStore.storeEntitySettings(realtimeContext, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));

        when(mockSnapshotCache.getSnapshot(eq(topologyId))).thenReturn(Optional.of(mockSnapshot));
        final Map<Long, Collection<Setting>> expectedMap =
                ImmutableMap.of(1L, Collections.emptyList());
        when(mockSnapshot.getFilteredSettings(eq(settingsFilter))).thenReturn(expectedMap);

        final Map<Long, Collection<Setting>> result = entitySettingStore.getEntitySettings(
            TopologySelection.newBuilder()
                .setTopologyId(topologyId)
                .build(), settingsFilter);
        verify(mockSnapshot).getFilteredSettings(eq(settingsFilter));
        assertEquals(expectedMap, result);
    }

    @Test
    public void testGetEntitySettingsNoIdAndNoContext() throws NoSettingsForTopologyException {
        entitySettingStore.storeEntitySettings(realtimeContext, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));

        when(mockSnapshotCache.getLatestSnapshot()).thenReturn(Optional.of(mockSnapshot));
        final Map<Long, Collection<Setting>> expectedMap =
                ImmutableMap.of(1L, Collections.emptyList());
        when(mockSnapshot.getFilteredSettings(eq(settingsFilter))).thenReturn(expectedMap);

        final Map<Long, Collection<Setting>> result = entitySettingStore.getEntitySettings(
                TopologySelection.newBuilder().build(), settingsFilter);
        verify(mockSnapshot).getFilteredSettings(eq(settingsFilter));
        assertEquals(expectedMap, result);
    }

    @Test
    public void testGetEntitySettingsContextButNoId() throws NoSettingsForTopologyException {
        entitySettingStore.storeEntitySettings(contextId, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));

        when(mockSnapshotCache.getLatestSnapshot()).thenReturn(Optional.of(mockSnapshot));
        final Map<Long, Collection<Setting>> expectedMap =
                ImmutableMap.of(1L, Collections.emptyList());
        when(mockSnapshot.getFilteredSettings(eq(settingsFilter))).thenReturn(expectedMap);

        final Map<Long, Collection<Setting>> result = entitySettingStore.getEntitySettings(
            TopologySelection.newBuilder()
                .setTopologyContextId(contextId)
                .build(), settingsFilter);
        verify(mockSnapshot).getFilteredSettings(eq(settingsFilter));
        assertEquals(expectedMap, result);
    }

    @Test(expected = NoSettingsForTopologyException.class)
    public void testGetEntitySettingsContextNotFound() throws NoSettingsForTopologyException {
        entitySettingStore.getEntitySettings(TopologySelection.newBuilder()
                .setTopologyContextId(contextId)
                .build(), settingsFilter);
    }

    @Test(expected = NoSettingsForTopologyException.class)
    public void testGetEntitySettingsTopologyNotFound() throws NoSettingsForTopologyException {
        entitySettingStore.storeEntitySettings(contextId, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));

        when(mockSnapshotCache.getSnapshot(eq(topologyId + 1))).thenReturn(Optional.empty());
        entitySettingStore.getEntitySettings(TopologySelection.newBuilder()
                .setTopologyContextId(contextId)
                .setTopologyId(topologyId + 1)
                .build(), settingsFilter);
    }

    @Test(expected = NoSettingsForTopologyException.class)
    public void testGetEntitySettingsNoLatestTopology() throws NoSettingsForTopologyException {
        entitySettingStore.storeEntitySettings(contextId, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));

        when(mockSnapshotCache.getLatestSnapshot()).thenReturn(Optional.empty());
        entitySettingStore.getEntitySettings(TopologySelection.newBuilder()
                .setTopologyContextId(contextId)
                .build(), settingsFilter);
    }

    @Test
    public void testSnapshotCacheAddAndGet() {
        final ContextSettingSnapshotCache cache = new ContextSettingSnapshotCache();
        Optional<EntitySettingSnapshot> oldSnapshot = cache.addSnapshot(7L, mockSnapshot);
        assertFalse(oldSnapshot.isPresent());
        assertEquals(mockSnapshot, cache.getSnapshot(7L).get());
    }

    @Test
    public void testSnapshotCacheGetNonExisting() {
        final ContextSettingSnapshotCache cache = new ContextSettingSnapshotCache();
        assertFalse(cache.getSnapshot(1233L).isPresent());
    }

    @Test
    public void testSnapshotCacheOverride() {
        final EntitySettingSnapshot snapshot1 = mock(EntitySettingSnapshot.class);
        final ContextSettingSnapshotCache cache = new ContextSettingSnapshotCache();
        cache.addSnapshot(7L, mockSnapshot);
        Optional<EntitySettingSnapshot> oldSnapshot = cache.addSnapshot(7L, snapshot1);
        assertEquals(mockSnapshot, oldSnapshot.get());
    }

    @Test
    public void testSnapshotCacheEviction() {
        final EntitySettingSnapshot snapshot1 = mock(EntitySettingSnapshot.class);
        final EntitySettingSnapshot snapshot2 = mock(EntitySettingSnapshot.class);
        final ContextSettingSnapshotCache cache = new ContextSettingSnapshotCache();
        cache.addSnapshot(7L, mockSnapshot);
        assertTrue(cache.getSnapshot(7L).isPresent());
        cache.addSnapshot(8L, snapshot1);
        assertTrue(cache.getSnapshot(7L).isPresent());
        cache.addSnapshot(9L, snapshot2);
        assertFalse(cache.getSnapshot(7L).isPresent());
    }

    @Test
    public void testSnapshotCacheGetLatest() {
        final EntitySettingSnapshot snapshot1 = mock(EntitySettingSnapshot.class);
        final ContextSettingSnapshotCache cache = new ContextSettingSnapshotCache();
        assertFalse(cache.getLatestSnapshot().isPresent());
        cache.addSnapshot(7L, mockSnapshot);
        assertEquals(mockSnapshot, cache.getLatestSnapshot().get());
        cache.addSnapshot(8L, snapshot1);
        assertEquals(snapshot1, cache.getLatestSnapshot().get());
    }

    @Test
    public void testSettingSnapshotGetAll() {
        final Setting setting = Setting.newBuilder()
                .setSettingSpecName("name1")
                .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
                .build();
        final EntitySettings id1Settings = EntitySettings.newBuilder()
                .setEntityOid(1L)
                .addUserSettings(setting)
                .build();

        final EntitySettingSnapshot snapshot = new EntitySettingSnapshot(Stream.of(id1Settings),
                Collections.emptyMap());
        final Map<Long, Collection<Setting>> results =
                snapshot.getFilteredSettings(EntitySettingFilter.getDefaultInstance());
        assertTrue(results.containsKey(id1Settings.getEntityOid()));
        assertThat(results.get(id1Settings.getEntityOid()), containsInAnyOrder(setting));
    }

    @Test
    public void testSettingSnapshotFilterById() {
        final Setting setting = Setting.newBuilder()
                .setSettingSpecName("name1")
                .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
                .build();
        final EntitySettings id1Settings = EntitySettings.newBuilder()
                .setEntityOid(1L)
                .addUserSettings(setting)
                .build();

        final EntitySettings id2Settings = EntitySettings.newBuilder()
                .setEntityOid(2L)
                .addUserSettings(setting)
                .build();

        final EntitySettingSnapshot snapshot = new EntitySettingSnapshot(
                Stream.of(id1Settings, id2Settings), Collections.emptyMap());
        final Map<Long, Collection<Setting>> results =
            snapshot.getFilteredSettings(EntitySettingFilter.newBuilder()
                .addEntities(1L)
                .build());
        assertTrue(results.containsKey(id1Settings.getEntityOid()));
        assertThat(results.get(id1Settings.getEntityOid()), containsInAnyOrder(setting));
    }

    @Test
    public void testSettingSnapshotFilterByNonExistingId() {
        final EntitySettingSnapshot snapshot = new EntitySettingSnapshot(
                Stream.empty(), Collections.emptyMap());
        final long entityId = 1;
        final Map<Long, Collection<Setting>> results =
                snapshot.getFilteredSettings(EntitySettingFilter.newBuilder()
                        .addEntities(entityId)
                        .build());
        assertTrue(results.containsKey(entityId));
        assertTrue(results.get(entityId).isEmpty());
    }

    @Test
    public void testSettingSnapshotDefault() {

        final EntitySettings settings = EntitySettings.newBuilder()
                .setEntityOid(2L)
                .setDefaultSettingPolicyId(settingPolicyWithFalseSetting.getId())
                .build();

        final EntitySettingSnapshot snapshot = new EntitySettingSnapshot(
                Stream.of(settings),
                ImmutableMap.of(settingPolicyWithFalseSetting.getId(),
                        settingPolicyWithFalseSetting));
        final Map<Long, Collection<Setting>> results =
                snapshot.getFilteredSettings(EntitySettingFilter.getDefaultInstance());

        // We expect to see the true setting for id 1, and the
        // false (default) setting for id 2.
        final Collection<Setting> settingsResult = results.get(settings.getEntityOid());
        assertNotNull(settingsResult);
        assertThat(settingsResult, containsInAnyOrder(falseSetting));
    }

    @Test
    public void testSettingSnapshotOverrideDefault() {
        final EntitySettings settings = EntitySettings.newBuilder()
                .setEntityOid(2L)
                .addUserSettings(trueSetting)
                .setDefaultSettingPolicyId(settingPolicyWithFalseSetting.getId())
                .build();

        final EntitySettingSnapshot snapshot = new EntitySettingSnapshot(
                Stream.of(settings),
                ImmutableMap.of(settingPolicyWithFalseSetting.getId(),
                        settingPolicyWithFalseSetting));
        final Map<Long, Collection<Setting>> results =
                snapshot.getFilteredSettings(EntitySettingFilter.getDefaultInstance());

        // We expect to see the true setting for id 1, and the
        // false (default) setting for id 2.
        final Collection<Setting> settingsResult = results.get(settings.getEntityOid());
        assertNotNull(settingsResult);
        assertThat(settingsResult, containsInAnyOrder(trueSetting));
    }

    @Test
    public void testSnapshotInconsistentSetting() {
        final EntitySettings settings = EntitySettings.newBuilder()
                .setEntityOid(7L)
                .setDefaultSettingPolicyId(1L)
                .build();

        // The default setting policy is NOT referring to a policy in the input map.
        final EntitySettingSnapshot snapshot = new EntitySettingSnapshot(
                Stream.of(settings), Collections.emptyMap());

        // Shouldn't have any settings in the snapshot.
        assertTrue(snapshot.getFilteredSettings(EntitySettingFilter.getDefaultInstance()).isEmpty());
    }

    @Test
    public void testSnapshotNoIdSetting() {
        final EntitySettings settings = EntitySettings.getDefaultInstance();
        final EntitySettingSnapshot snapshot = new EntitySettingSnapshot(
                Stream.of(settings), Collections.emptyMap());
        // Shouldn't have any settings in the snapshot.
        assertTrue(snapshot.getFilteredSettings(EntitySettingFilter.getDefaultInstance()).isEmpty());
    }
}
