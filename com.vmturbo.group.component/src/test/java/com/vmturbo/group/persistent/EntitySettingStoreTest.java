package com.vmturbo.group.persistent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.TopologySelection;
import com.vmturbo.group.persistent.EntitySettingStore.ContextSettingSnapshotCache;
import com.vmturbo.group.persistent.EntitySettingStore.ContextSettingSnapshotCacheFactory;
import com.vmturbo.group.persistent.EntitySettingStore.EntitySettingSnapshot;
import com.vmturbo.group.persistent.EntitySettingStore.EntitySettingSnapshotFactory;
import com.vmturbo.group.persistent.EntitySettingStore.NoSettingsForTopologyException;

public class EntitySettingStoreTest {

    private final long realtimeContext = 7L;

    private EntitySettingSnapshotFactory snapshotFactory = mock(EntitySettingSnapshotFactory.class);

    private ContextSettingSnapshotCacheFactory cacheFactory = mock(ContextSettingSnapshotCacheFactory.class);

    private EntitySettingStore settingStore =
            new EntitySettingStore(realtimeContext, snapshotFactory, cacheFactory);

    private final EntitySettingFilter settingsFilter = EntitySettingFilter.newBuilder()
            .addEntities(1)
            .build();

    private final long contextId = 10L;
    private final long topologyId = 11L;

    private EntitySettingSnapshot mockSnapshot = mock(EntitySettingSnapshot.class);
    private ContextSettingSnapshotCache mockSnapshotCache = mock(ContextSettingSnapshotCache.class);

    @Before
    public void setup() {
        when(mockSnapshotCache.getSnapshot(anyLong())).thenReturn(Optional.empty());
        when(mockSnapshotCache.addSnapshot(anyLong(), any())).thenReturn(Optional.empty());
        when(snapshotFactory.createSnapshot(any())).thenReturn(mockSnapshot);
        when(cacheFactory.newSnapshotCache()).thenReturn(mockSnapshotCache);
    }

    /**
     * Test to make sure that the non-test constructor doesn't generate any errors.
     */
    @Test
    public void testEntityStoreRealConstructor() {
        new EntitySettingStore(7L);
    }

    @Test
    public void testStoreEntitySettings() {
        settingStore.storeEntitySettings(contextId, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));

        verify(snapshotFactory).createSnapshot(any());
        verify(cacheFactory).newSnapshotCache();
        verify(mockSnapshotCache).addSnapshot(eq(topologyId), eq(mockSnapshot));
    }

    @Test
    public void testDuplicateStoreEntitySettings() {
        settingStore.storeEntitySettings(contextId, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));
        settingStore.storeEntitySettings(contextId, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));

        verify(snapshotFactory, times(2)).createSnapshot(any());
        verify(cacheFactory).newSnapshotCache();
        verify(mockSnapshotCache, times(2)).addSnapshot(eq(topologyId), eq(mockSnapshot));
    }

    @Test
    public void testStoreEntitySettingsMultipleTopologies() {
        settingStore.storeEntitySettings(contextId, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));
        settingStore.storeEntitySettings(contextId, topologyId + 1,
                Stream.of(EntitySettings.getDefaultInstance()));

        verify(snapshotFactory, times(2)).createSnapshot(any());
        verify(cacheFactory).newSnapshotCache();
        verify(mockSnapshotCache).addSnapshot(eq(topologyId), eq(mockSnapshot));
        verify(mockSnapshotCache).addSnapshot(eq(topologyId + 1), eq(mockSnapshot));
    }

    @Test
    public void testGetEntitySettings() throws NoSettingsForTopologyException {
        settingStore.storeEntitySettings(contextId, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));

        when(mockSnapshotCache.getSnapshot(eq(topologyId))).thenReturn(Optional.of(mockSnapshot));
        final Map<Long, List<Setting>> expectedMap =
                ImmutableMap.of(1L, Collections.emptyList());
        when(mockSnapshot.getFilteredSettings(eq(settingsFilter))).thenReturn(expectedMap);

        final Map<Long, List<Setting>> result = settingStore.getEntitySettings(TopologySelection.newBuilder()
                .setTopologyContextId(contextId)
                .setTopologyId(topologyId)
                .build(), settingsFilter);

        verify(mockSnapshot).getFilteredSettings(eq(settingsFilter));
        assertEquals(expectedMap, result);
    }

    @Test
    public void testGetEntitySettingsIdButNoContext() throws NoSettingsForTopologyException {
        settingStore.storeEntitySettings(realtimeContext, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));

        when(mockSnapshotCache.getSnapshot(eq(topologyId))).thenReturn(Optional.of(mockSnapshot));
        final Map<Long, List<Setting>> expectedMap =
                ImmutableMap.of(1L, Collections.emptyList());
        when(mockSnapshot.getFilteredSettings(eq(settingsFilter))).thenReturn(expectedMap);

        final Map<Long, List<Setting>> result = settingStore.getEntitySettings(TopologySelection.newBuilder()
                .setTopologyId(topologyId)
                .build(), settingsFilter);
        verify(mockSnapshot).getFilteredSettings(eq(settingsFilter));
        assertEquals(expectedMap, result);
    }

    @Test
    public void testGetEntitySettingsNoIdAndNoContext() throws NoSettingsForTopologyException {
        settingStore.storeEntitySettings(realtimeContext, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));

        when(mockSnapshotCache.getLatestSnapshot()).thenReturn(Optional.of(mockSnapshot));
        final Map<Long, List<Setting>> expectedMap =
                ImmutableMap.of(1L, Collections.emptyList());
        when(mockSnapshot.getFilteredSettings(eq(settingsFilter))).thenReturn(expectedMap);

        final Map<Long, List<Setting>> result = settingStore.getEntitySettings(TopologySelection.newBuilder()
                .build(), settingsFilter);
        verify(mockSnapshot).getFilteredSettings(eq(settingsFilter));
        assertEquals(expectedMap, result);
    }

    @Test
    public void testGetEntitySettingsContextButNoId() throws NoSettingsForTopologyException {
        settingStore.storeEntitySettings(contextId, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));

        when(mockSnapshotCache.getLatestSnapshot()).thenReturn(Optional.of(mockSnapshot));
        final Map<Long, List<Setting>> expectedMap =
                ImmutableMap.of(1L, Collections.emptyList());
        when(mockSnapshot.getFilteredSettings(eq(settingsFilter))).thenReturn(expectedMap);

        final Map<Long, List<Setting>> result = settingStore.getEntitySettings(TopologySelection.newBuilder()
                .setTopologyContextId(contextId)
                .build(), settingsFilter);
        verify(mockSnapshot).getFilteredSettings(eq(settingsFilter));
        assertEquals(expectedMap, result);
    }

    @Test(expected = NoSettingsForTopologyException.class)
    public void testGetEntitySettingsContextNotFound() throws NoSettingsForTopologyException {
        settingStore.getEntitySettings(TopologySelection.newBuilder()
                .setTopologyContextId(contextId)
                .build(), settingsFilter);
    }

    @Test(expected = NoSettingsForTopologyException.class)
    public void testGetEntitySettingsTopologyNotFound() throws NoSettingsForTopologyException {
        settingStore.storeEntitySettings(contextId, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));

        when(mockSnapshotCache.getSnapshot(eq(topologyId + 1))).thenReturn(Optional.empty());
        settingStore.getEntitySettings(TopologySelection.newBuilder()
                .setTopologyContextId(contextId)
                .setTopologyId(topologyId + 1)
                .build(), settingsFilter);
    }

    @Test(expected = NoSettingsForTopologyException.class)
    public void testGetEntitySettingsNoLatestTopology() throws NoSettingsForTopologyException {
        settingStore.storeEntitySettings(contextId, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));

        when(mockSnapshotCache.getLatestSnapshot()).thenReturn(Optional.empty());
        settingStore.getEntitySettings(TopologySelection.newBuilder()
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
                .addSettings(setting)
                .build();

        final EntitySettingSnapshot snapshot = new EntitySettingSnapshot(Stream.of(id1Settings));
        final Map<Long, List<Setting>> results =
                snapshot.getFilteredSettings(EntitySettingFilter.getDefaultInstance());
        assertEquals(ImmutableMap.of(1L, Collections.singletonList(setting)), results);
    }

    @Test
    public void testSettingSnapshotFilterById() {
        final Setting setting = Setting.newBuilder()
                .setSettingSpecName("name1")
                .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
                .build();
        final EntitySettings id1Settings = EntitySettings.newBuilder()
                .setEntityOid(1L)
                .addSettings(setting)
                .build();

        final EntitySettings id2Settings = EntitySettings.newBuilder()
                .setEntityOid(2L)
                .addSettings(setting)
                .build();

        final EntitySettingSnapshot snapshot = new EntitySettingSnapshot(
                Stream.of(id1Settings, id2Settings));
        final Map<Long, List<Setting>> results =
            snapshot.getFilteredSettings(EntitySettingFilter.newBuilder()
                .addEntities(1L)
                .addEntities(3L)
                .build());
        assertEquals(ImmutableMap.of(1L, Collections.singletonList(setting),
                3L, Collections.emptyList()), results);
    }
}
