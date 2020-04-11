package com.vmturbo.group.setting;

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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.assertj.core.util.Lists;
import org.jooq.exception.DataAccessException;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings.SettingToPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.TopologySelection;
import com.vmturbo.group.setting.EntitySettingStore.ContextSettingSnapshotCache;
import com.vmturbo.group.setting.EntitySettingStore.ContextSettingSnapshotCacheFactory;
import com.vmturbo.group.setting.EntitySettingStore.EntitySettingSnapshot;
import com.vmturbo.group.setting.EntitySettingStore.EntitySettingSnapshotFactory;
import com.vmturbo.group.setting.EntitySettingStore.NoSettingsForTopologyException;

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

    final SettingToPolicyId trueSettingToPolicyId =
            SettingToPolicyId.newBuilder()
                    .setSetting(trueSetting)
                    .addSettingPolicyId(1L)
                    .build();

    private final Setting falseSetting = Setting.newBuilder(trueSetting)
            .setBooleanSettingValue(BooleanSettingValue.newBuilder()
                    .setValue(false))
            .build();

    private final SettingPolicy settingPolicyWithFalseSetting = SettingPolicy.newBuilder()
            .setId(7L)
            .setSettingPolicyType(Type.DEFAULT)
            .setInfo(SettingPolicyInfo.newBuilder()
                    .addSettings(falseSetting))
            .build();

    @Before
    public void setup() throws Exception {
        when(mockSnapshotCache.getSnapshot(anyLong())).thenReturn(Optional.empty());
        when(mockSnapshotCache.addSnapshot(anyLong(), any())).thenReturn(Optional.empty());
        when(snapshotFactory.createSnapshot(any(), any())).thenReturn(mockSnapshot);
        when(cacheFactory.newSnapshotCache()).thenReturn(mockSnapshotCache);
        when(settingStore.getSettingPolicies(eq(SettingPolicyFilter.newBuilder()
                .withType(Type.DEFAULT)
                .build())))
            .thenReturn(Collections.emptyList())
            .thenReturn(Collections.emptyList());
    }

    /**
     * Test to make sure that the non-test constructor doesn't generate any errors.
     */
    @Test
    public void testEntityStoreRealConstructor() {
        new EntitySettingStore(7L, settingStore);
    }

    @Test
    public void testStoreEntitySettings() throws Exception {
        entitySettingStore.storeEntitySettings(contextId, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));

        verify(snapshotFactory).createSnapshot(any(), eq(Collections.emptyMap()));
        verify(cacheFactory).newSnapshotCache();
        verify(mockSnapshotCache).addSnapshot(eq(topologyId), eq(mockSnapshot));
    }

    @Test
    public void testDuplicateStoreEntitySettings() throws Exception {
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
    public void testStoreEntitySettingsDefaults() throws Exception {
        final SettingPolicy settingPolicy = SettingPolicy.newBuilder()
            .setId(7L)
            .setInfo(SettingPolicyInfo.getDefaultInstance())
            .build();
        when(settingStore.getSettingPolicies(SettingPolicyFilter.newBuilder()
                .withType(Type.DEFAULT)
                .build()))
            .thenReturn(Collections.singletonList(settingPolicy));

        entitySettingStore.storeEntitySettings(contextId, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));
        verify(snapshotFactory).createSnapshot(any(), eq(ImmutableMap.of(7L, settingPolicy)));
        verify(cacheFactory).newSnapshotCache();
        verify(mockSnapshotCache).addSnapshot(eq(topologyId), eq(mockSnapshot));
    }

    @Test(expected = DataAccessException.class)
    public void testStoreEntitySettingsDBException() throws Exception {
        when(settingStore.getSettingPolicies(SettingPolicyFilter.newBuilder()
                .withType(Type.DEFAULT)
                .build()))
            .thenThrow(DataAccessException.class);
        entitySettingStore.storeEntitySettings(contextId, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));
    }

    @Test
    public void testStoreEntitySettingsMultipleTopologies() throws Exception {
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
    public void testGetEntitySettings() throws Exception {
        entitySettingStore.storeEntitySettings(contextId, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));

        when(mockSnapshotCache.getSnapshot(eq(topologyId))).thenReturn(Optional.of(mockSnapshot));
        final Map<Long, Collection<SettingToPolicyId>> expectedMap =
                ImmutableMap.of(1L, Collections.emptyList());
        when(mockSnapshot.getFilteredSettings(eq(settingsFilter))).thenReturn(expectedMap);

        final Map<Long, Collection<SettingToPolicyId>> result =
            entitySettingStore.getEntitySettings(TopologySelection.newBuilder()
                .setTopologyContextId(contextId)
                .setTopologyId(topologyId)
                .build(), settingsFilter);

        verify(mockSnapshot).getFilteredSettings(eq(settingsFilter));
        assertEquals(expectedMap, result);
    }

    @Test
    public void testGetEntitySettingsIdButNoContext() throws Exception {
        entitySettingStore.storeEntitySettings(realtimeContext, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));

        when(mockSnapshotCache.getSnapshot(eq(topologyId))).thenReturn(Optional.of(mockSnapshot));
        final Map<Long, Collection<SettingToPolicyId>> expectedMap =
                ImmutableMap.of(1L, Collections.emptyList());
        when(mockSnapshot.getFilteredSettings(eq(settingsFilter))).thenReturn(expectedMap);

        final Map<Long, Collection<SettingToPolicyId>> result = entitySettingStore.getEntitySettings(
            TopologySelection.newBuilder()
                .setTopologyId(topologyId)
                .build(), settingsFilter);
        verify(mockSnapshot).getFilteredSettings(eq(settingsFilter));
        assertEquals(expectedMap, result);
    }

    @Test
    public void testGetEntitySettingsNoIdAndNoContext() throws Exception {
        entitySettingStore.storeEntitySettings(realtimeContext, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));

        when(mockSnapshotCache.getLatestSnapshot()).thenReturn(Optional.of(mockSnapshot));
        final Map<Long, Collection<SettingToPolicyId>> expectedMap =
                ImmutableMap.of(1L, Collections.emptyList());
        when(mockSnapshot.getFilteredSettings(eq(settingsFilter))).thenReturn(expectedMap);

        final Map<Long, Collection<SettingToPolicyId>> result = entitySettingStore.getEntitySettings(
                TopologySelection.newBuilder().build(), settingsFilter);
        verify(mockSnapshot).getFilteredSettings(eq(settingsFilter));
        assertEquals(expectedMap, result);
    }

    @Test
    public void testGetEntitySettingsContextButNoId() throws Exception {
        entitySettingStore.storeEntitySettings(contextId, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));

        when(mockSnapshotCache.getLatestSnapshot()).thenReturn(Optional.of(mockSnapshot));
        final Map<Long, Collection<SettingToPolicyId>> expectedMap =
                ImmutableMap.of(1L, Collections.emptyList());
        when(mockSnapshot.getFilteredSettings(eq(settingsFilter))).thenReturn(expectedMap);

        final Map<Long, Collection<SettingToPolicyId>> result = entitySettingStore.getEntitySettings(
            TopologySelection.newBuilder()
                .setTopologyContextId(contextId)
                .build(), settingsFilter);
        verify(mockSnapshot).getFilteredSettings(eq(settingsFilter));
        assertEquals(expectedMap, result);
    }

    @Test(expected = NoSettingsForTopologyException.class)
    public void testGetEntitySettingsContextNotFound() throws Exception {
        entitySettingStore.getEntitySettings(TopologySelection.newBuilder()
                .setTopologyContextId(contextId)
                .build(), settingsFilter);
    }

    @Test(expected = NoSettingsForTopologyException.class)
    public void testGetEntitySettingsTopologyNotFound() throws Exception {
        entitySettingStore.storeEntitySettings(contextId, topologyId,
                Stream.of(EntitySettings.getDefaultInstance()));

        when(mockSnapshotCache.getSnapshot(eq(topologyId + 1))).thenReturn(Optional.empty());
        entitySettingStore.getEntitySettings(TopologySelection.newBuilder()
                .setTopologyContextId(contextId)
                .setTopologyId(topologyId + 1)
                .build(), settingsFilter);
    }

    @Test(expected = NoSettingsForTopologyException.class)
    public void testGetEntitySettingsNoLatestTopology() throws Exception {
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
        final SettingToPolicyId settingToPolicyId =
            SettingToPolicyId.newBuilder()
                .setSetting(setting)
                .addSettingPolicyId(1L)
                .build();
        final EntitySettings id1Settings = EntitySettings.newBuilder()
            .setEntityOid(1L)
            .addUserSettings(settingToPolicyId)
            .build();

        final EntitySettingSnapshot snapshot = new EntitySettingSnapshot(Stream.of(id1Settings),
            Collections.emptyMap());
        final Map<Long, Collection<SettingToPolicyId>> results =
            snapshot.getFilteredSettings(EntitySettingFilter.getDefaultInstance());
        assertTrue(results.containsKey(id1Settings.getEntityOid()));
        assertThat(getSettings(results.get(id1Settings.getEntityOid())), containsInAnyOrder(setting));
    }

    @Test
    public void testSettingSnapshotFilterById() {
        final Setting setting = Setting.newBuilder()
                .setSettingSpecName("name1")
                .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
                .build();
        final SettingToPolicyId settingToPolicyId =
                SettingToPolicyId.newBuilder()
                        .setSetting(setting)
                        .addSettingPolicyId(1L)
                        .build();

        final EntitySettings id1Settings = EntitySettings.newBuilder()
                .setEntityOid(1L)
                .addUserSettings(settingToPolicyId)
                .build();

        final EntitySettings id2Settings = EntitySettings.newBuilder()
                .setEntityOid(2L)
                .addUserSettings(settingToPolicyId)
                .build();

        final EntitySettingSnapshot snapshot = new EntitySettingSnapshot(
                Stream.of(id1Settings, id2Settings), Collections.emptyMap());
        final Map<Long, Collection<SettingToPolicyId>> results =
            snapshot.getFilteredSettings(EntitySettingFilter.newBuilder()
                .addEntities(1L)
                .build());
        assertTrue(results.containsKey(id1Settings.getEntityOid()));
        assertThat(getSettings(results.get(id1Settings.getEntityOid())),
                containsInAnyOrder(setting));
    }

    @Test
    public void testSettingSnapshotFilterByDefaultPolicy() {
        final Setting setting = Setting.newBuilder()
            .setSettingSpecName("name1")
            .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
            .build();
        final Setting setting2 = Setting.newBuilder()
            .setSettingSpecName("name2")
            .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
            .build();
        final long defaultPolicyId = 1L;

        final SettingToPolicyId userSetting =
            SettingToPolicyId.newBuilder()
                .setSetting(setting2)
                .addSettingPolicyId(1111L)
                .build();

        final EntitySettings id1Settings = EntitySettings.newBuilder()
            .setEntityOid(1L)
            .addUserSettings(userSetting)
            .setDefaultSettingPolicyId(defaultPolicyId)
            .build();

        final SettingPolicy defaultSettingPolicy = SettingPolicy.newBuilder()
            .setInfo(SettingPolicyInfo.newBuilder()
                .addSettings(setting)
                .addSettings(setting2))
            .build();

        final EntitySettingSnapshot snapshot = new EntitySettingSnapshot(
            Stream.of(id1Settings), Collections.singletonMap(defaultPolicyId, defaultSettingPolicy));
        final Map<Long, Collection<SettingToPolicyId>> results =
            snapshot.getFilteredSettings(EntitySettingFilter.newBuilder()
                .setPolicyId(defaultPolicyId)
                .build());
        assertTrue(results.containsKey(id1Settings.getEntityOid()));
        // Should only have the setting that's unique to the default policy. Not the user.
        assertThat(getSettings(results.get(id1Settings.getEntityOid())), containsInAnyOrder(setting));
    }

    @Test
    public void testSettingSnapshotFilterByUserPolicy() {
        final Setting setting = Setting.newBuilder()
            .setSettingSpecName("name1")
            .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
            .build();
        final long defaultPolicyId = 1L;

        final SettingToPolicyId userSetting =
            SettingToPolicyId.newBuilder()
                .setSetting(Setting.newBuilder()
                    .setSettingSpecName("name2")
                    .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance()))
                .addSettingPolicyId(1111L)
                .build();
        final SettingToPolicyId otherUserSetting =
            SettingToPolicyId.newBuilder()
                .setSetting(Setting.newBuilder()
                    .setSettingSpecName("name3")
                    .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance()))
                .addSettingPolicyId(2222L)
                .build();

        final EntitySettings id1Settings = EntitySettings.newBuilder()
            .setEntityOid(1L)
            .addUserSettings(userSetting)
            .addUserSettings(otherUserSetting)
            .setDefaultSettingPolicyId(defaultPolicyId)
            .build();

        final SettingPolicy defaultSettingPolicy = SettingPolicy.newBuilder()
            .setInfo(SettingPolicyInfo.newBuilder()
                .addSettings(setting))
            .build();

        final EntitySettingSnapshot snapshot = new EntitySettingSnapshot(
            Stream.of(id1Settings), Collections.singletonMap(defaultPolicyId, defaultSettingPolicy));
        final Map<Long, Collection<SettingToPolicyId>> results =
            snapshot.getFilteredSettings(EntitySettingFilter.newBuilder()
                .setPolicyId(userSetting.getSettingPolicyId(0))
                .build());
        assertTrue(results.containsKey(id1Settings.getEntityOid()));

        // Should only have the setting from the specific user policy. Not the other user policy,
        // and not the default.
        assertThat(getSettings(results.get(id1Settings.getEntityOid())),
            containsInAnyOrder(userSetting.getSetting()));
    }

    @Test
    public void testSettingSnapshotFilterRestrictName() {
        final Setting setting = Setting.newBuilder()
            .setSettingSpecName("name1")
            .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
            .build();
        final long defaultPolicyId = 1L;

        final SettingToPolicyId userSetting =
            SettingToPolicyId.newBuilder()
                .setSetting(Setting.newBuilder()
                    .setSettingSpecName("name2")
                    .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance()))
                .addSettingPolicyId(1111L)
                .build();
        final SettingToPolicyId otherUserSetting =
            SettingToPolicyId.newBuilder()
                .setSetting(Setting.newBuilder()
                    .setSettingSpecName("name3")
                    .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance()))
                .addSettingPolicyId(2222L)
                .build();

        final EntitySettings id1Settings = EntitySettings.newBuilder()
            .setEntityOid(1L)
            .addUserSettings(userSetting)
            .addUserSettings(otherUserSetting)
            .setDefaultSettingPolicyId(defaultPolicyId)
            .build();

        final SettingPolicy defaultSettingPolicy = SettingPolicy.newBuilder()
            .setInfo(SettingPolicyInfo.newBuilder()
                .addSettings(setting))
            .build();

        final EntitySettingSnapshot snapshot = new EntitySettingSnapshot(
            Stream.of(id1Settings), Collections.singletonMap(defaultPolicyId, defaultSettingPolicy));
        final Map<Long, Collection<SettingToPolicyId>> results =
            snapshot.getFilteredSettings(EntitySettingFilter.newBuilder()
                .addSettingName(setting.getSettingSpecName())
                .addSettingName(userSetting.getSetting().getSettingSpecName())
                .build());
        assertTrue(results.containsKey(id1Settings.getEntityOid()));

        // Should only have the setting from the specific user policy and the default policy.
        // Not the other user policy.
        assertThat(getSettings(results.get(id1Settings.getEntityOid())),
            containsInAnyOrder(setting, userSetting.getSetting()));
    }

    @Test
    public void testSettingSnapshotFilterByNonExistingId() {
        final EntitySettingSnapshot snapshot = new EntitySettingSnapshot(
                Stream.empty(), Collections.emptyMap());
        final long entityId = 1;
        final Map<Long, Collection<SettingToPolicyId>> results =
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
        final Map<Long, Collection<SettingToPolicyId>> results =
                snapshot.getFilteredSettings(EntitySettingFilter.getDefaultInstance());

        // We expect to see the true setting for id 1, and the
        // false (default) setting for id 2.
        final Collection<SettingToPolicyId> settingsResult =
                results.get(settings.getEntityOid());
        assertNotNull(settingsResult);
        assertThat(getSettings(settingsResult), containsInAnyOrder(falseSetting));
    }

    @Test
    public void testSettingSnapshotOverrideDefault() {
        final EntitySettings settings = EntitySettings.newBuilder()
                .setEntityOid(2L)
                .addUserSettings(trueSettingToPolicyId)
                .setDefaultSettingPolicyId(settingPolicyWithFalseSetting.getId())
                .build();

        final EntitySettingSnapshot snapshot = new EntitySettingSnapshot(
                Stream.of(settings),
                ImmutableMap.of(settingPolicyWithFalseSetting.getId(),
                        settingPolicyWithFalseSetting));
        final Map<Long, Collection<SettingToPolicyId>> results =
                snapshot.getFilteredSettings(EntitySettingFilter.getDefaultInstance());

        // We expect to see the true setting for id 1, and the
        // false (default) setting for id 2.
        final Collection<SettingToPolicyId> settingsResult =
                results.get(settings.getEntityOid());
        assertNotNull(settingsResult);
        assertThat(getSettings(settingsResult), containsInAnyOrder(trueSetting));
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

    // Verify we don't create new SettingToPolicyId object for same defaultSettingPolicy
    @Test
    public void testAcquireSettingToPolicyId() {
        final EntitySettings settings = EntitySettings.newBuilder()
            .setEntityOid(2L)
            .setDefaultSettingPolicyId(settingPolicyWithFalseSetting.getId())
            .build();

        final EntitySettingSnapshot snapshot = new EntitySettingSnapshot(
            Stream.of(settings),
            ImmutableMap.of(settingPolicyWithFalseSetting.getId(),
                settingPolicyWithFalseSetting));
        final Map<Long, Collection<SettingToPolicyId>> results =
            snapshot.getFilteredSettings(EntitySettingFilter.getDefaultInstance());

        // We expect to see the true setting for id 1, and the
        // false (default) setting for id 2.
        final Collection<SettingToPolicyId> settingsResult =
            results.get(settings.getEntityOid());
        assertNotNull(settingsResult);
        assertThat(getSettings(settingsResult), containsInAnyOrder(falseSetting));

        // Get again to assure we don't create new SettingToPolicyId object
        final Map<Long, Collection<SettingToPolicyId>> resultsNext =
            snapshot.getFilteredSettings(EntitySettingFilter.getDefaultInstance());
        final Collection<SettingToPolicyId> settingsResultNext =
            resultsNext.get(settings.getEntityOid());
        // using == to assert they are exactly same object
        assertTrue(Lists.newArrayList(settingsResult).get(0) ==
            Lists.newArrayList(settingsResultNext).get(0));
    }

    private static List<Setting> getSettings(Collection<SettingToPolicyId> settingToPolicyIds) {
        return settingToPolicyIds.stream()
                .map(settingToPolicyId -> settingToPolicyId.getSetting())
                .collect(Collectors.toList());
    }
}
