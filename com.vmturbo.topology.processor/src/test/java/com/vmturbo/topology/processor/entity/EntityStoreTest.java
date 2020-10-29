package com.vmturbo.topology.processor.entity;

import static com.vmturbo.platform.common.builders.CommodityBuilders.storageAmount;
import static com.vmturbo.platform.common.builders.CommodityBuilders.vCpuMHz;
import static com.vmturbo.platform.common.builders.EntityBuilders.physicalMachine;
import static com.vmturbo.platform.common.builders.EntityBuilders.storage;
import static com.vmturbo.platform.common.builders.EntityBuilders.virtualMachine;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.identity.exceptions.IdentityServiceException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.topology.processor.api.server.TopologyProcessorNotificationSender;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.stitching.TopologyStitchingGraph;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.TargetStoreListener;

/**
 * Test the {@link EntityStore} methods.
 */
public class EntityStoreTest {
    private final long targetId = 1234;

    private final TargetStore targetStore = Mockito.mock(TargetStore.class);

    private final IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);

    private final TopologyProcessorNotificationSender sender = Mockito.mock(TopologyProcessorNotificationSender.class);

    private EntityStore entityStore = new EntityStore(targetStore, identityProvider,
        sender, Clock.systemUTC());

    /**
     * Expected exception rule.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Test querying entities added to the repository.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testEntitiesDiscovered() throws Exception {
        final String id1 = "en-hypervisorTarget";
        final EntityDTO entity1 = EntityDTO.newBuilder().setEntityType(EntityType.VIRTUAL_MACHINE)
                .setId(id1).build();
        Map<Long, EntityDTO> entitiesMap = ImmutableMap.of(1L, entity1);

        // Pretend that any target exists
        when(targetStore.getTarget(anyLong())).thenReturn(Optional.of(Mockito.mock(Target.class)));

        addEntities(entitiesMap);

        Assert.assertTrue(entityStore.getTargetEntityIdMap(targetId).isPresent());
        Assert.assertTrue(entityStore.getEntity(1L).isPresent());
        assertEquals(entitiesMap, entityStore.discoveredByTarget(targetId));
    }

    /**
     * Test that incremental discovery response for different targets are cached.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testIncrementalEntitiesCached() throws Exception {
        // Pretend that any target exists
        when(targetStore.getTarget(anyLong())).thenReturn(Optional.of(Mockito.mock(Target.class)));

        final long targetId1 = 2001;
        final long targetId2 = 2002;
        final long oid1 = 1L;
        final long oid2 = 2L;
        final EntityDTO entity1 = EntityDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE)
            .setId("pm1")
            .build();
        final EntityDTO entity2 = EntityDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE)
            .setId("pm2")
            .build();
        final EntityDTO entity3 = EntityDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE)
            .setId("pm1")
            .setMaintenance(true)
            .build();
        Map<Long, EntityDTO> entitiesMap1 = ImmutableMap.of(oid1, entity1);

        final int messageId1 = 0;
        final int messageId2 = 1;
        final int messageId3 = 2;

        // target 1: first incremental discovery
        // before
        assertFalse(entityStore.getIncrementalEntities(targetId1).isPresent());
        addEntities(entitiesMap1, targetId1, 0, DiscoveryType.INCREMENTAL, messageId1);
        // after
        assertTrue(entityStore.getIncrementalEntities(targetId1).isPresent());
        SortedMap<Integer, EntityDTO> entitiesOrderedByDiscovery1 =
            entityStore.getIncrementalEntities(targetId1).get().getEntitiesInDiscoveryOrder(oid1);
        assertEquals(1, entitiesOrderedByDiscovery1.size());
        assertEquals(entity1, entitiesOrderedByDiscovery1.get(messageId1));

        // target 2: first incremental discovery
        // before
        assertFalse(entityStore.getIncrementalEntities(targetId2).isPresent());
        Map<Long, EntityDTO> entitiesMap2 = ImmutableMap.of(oid2, entity2);
        addEntities(entitiesMap2, targetId2, 0, DiscoveryType.INCREMENTAL, messageId2);
        // after
        assertTrue(entityStore.getIncrementalEntities(targetId2).isPresent());
        // incremental cache for target1 is still there
        assertTrue(entityStore.getIncrementalEntities(targetId1).isPresent());
        SortedMap<Integer, EntityDTO> entitiesOrderedByDiscovery2 =
            entityStore.getIncrementalEntities(targetId2).get().getEntitiesInDiscoveryOrder(oid2);
        assertEquals(1, entitiesOrderedByDiscovery2.size());
        assertEquals(entity2, entitiesOrderedByDiscovery2.get(messageId2));

        // target 1: second incremental discovery
        // before
        Map<Long, EntityDTO> entitiesMap3 = ImmutableMap.of(oid1, entity3);
        addEntities(entitiesMap3, targetId1, 0, DiscoveryType.INCREMENTAL, messageId3);
        // after
        assertTrue(entityStore.getIncrementalEntities(targetId1).isPresent());
        assertTrue(entityStore.getIncrementalEntities(targetId2).isPresent());
        // now incremental cache for target1 contains two entries, from two different discoveries,
        SortedMap<Integer, EntityDTO> entitiesOrderedByDiscovery3 =
            entityStore.getIncrementalEntities(targetId1).get().getEntitiesInDiscoveryOrder(oid1);

        assertEquals(2, entitiesOrderedByDiscovery3.size());
        assertEquals(entity1, entitiesOrderedByDiscovery3.get(messageId1));
        assertEquals(entity3, entitiesOrderedByDiscovery3.get(messageId3));
        // verify the discovery order is preserved
        assertEquals(new ArrayList<>(entitiesOrderedByDiscovery3.keySet()),
            Lists.newArrayList(messageId1, messageId3));
    }

    /**
     * Test getting a missing entity.
     */
    @Test
    public void testGetWhenAbsent() {
        Assert.assertFalse(entityStore.getTargetEntityIdMap(targetId).isPresent());
        Assert.assertFalse(entityStore.getEntity(1L).isPresent());
    }

    /**
     * Tests for target removal after some data is reported for the target. The {@link EntityStore} is
     * expected to remove all its data after target has been removed.
     *
     * @throws Exception on exceptions occur.
     */
    @Test
    public void testTargetRemoval() throws Exception {
        final String id1 = "en-hypervisorTarget";
        final EntityDTO entity1 = EntityDTO.newBuilder().setEntityType(EntityType.VIRTUAL_MACHINE)
                .setId(id1).build();

        // Pretend that any target exists
        when(targetStore.getTarget(anyLong())).thenReturn(Optional.of(Mockito.mock(Target.class)));

        addEntities(ImmutableMap.of(1L, entity1));

        Assert.assertTrue(entityStore.getTargetEntityIdMap(targetId).isPresent());
        Assert.assertTrue(entityStore.getEntity(1L).isPresent());

        final ArgumentCaptor<TargetStoreListener> captor =
                ArgumentCaptor.forClass(TargetStoreListener.class);
        Mockito.verify(targetStore).addListener(captor.capture());
        final TargetStoreListener storeListener = captor.getValue();
        final Target target = Mockito.mock(Target.class);
        Mockito.when(target.getId()).thenReturn(targetId);
        storeListener.onTargetRemoved(target);

        Assert.assertFalse(entityStore.getTargetEntityIdMap(targetId).isPresent());
        Assert.assertFalse(entityStore.getEntity(1L).isPresent());
    }

    /**
     * Tests {@link EntityStore#getEntityIdByLocalId} method.
     *
     * @throws TargetNotFoundException Should not happen.
     * @throws IdentityServiceException Should not happen.
     */
    @Test
    public void testGetEntityIdByLocalId()
            throws TargetNotFoundException, IdentityServiceException {

        // Pretend that any target exists
        when(targetStore.getTarget(anyLong())).thenReturn(Optional.of(Mockito.mock(Target.class)));

        // Add entity 1 from target 1
        final String localId1 = "localId1";
        final long id1 = 1L;
        addVmEntity(id1, localId1, 12345L);

        // Add entity 2 from target 2
        final String localId2 = "localId2";
        final long id2 = 2L;
        addVmEntity(id2, localId2, 23456L);

        // Check entity 1
        final Optional<Long> id1Opt = entityStore.getEntityIdByLocalId(localId1);
        assertTrue(id1Opt.isPresent());
        assertEquals(id1, id1Opt.get().longValue());

        // Check entity 2
        final Optional<Long> id2Opt = entityStore.getEntityIdByLocalId(localId2);
        assertTrue(id2Opt.isPresent());
        assertEquals(id2, id2Opt.get().longValue());

        // Check not existing entity
        assertFalse(entityStore.getEntityIdByLocalId("nonExistingLocalId").isPresent());
    }

    private void addVmEntity(final long id, final String localId, final long targetId)
            throws TargetNotFoundException, IdentityServiceException {
        final EntityDTO entity = EntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .setId(localId)
                .build();
        addEntities(ImmutableMap.of(id, entity), targetId, 0, DiscoveryType.FULL, 0);
    }

    /**
     * Tests construct stitching graph for single target.
     *
     * @throws IdentityServiceException on service exceptions
     * @throws TargetNotFoundException if taret not found
     */
    @Test
    public void testConstructStitchingGraphSingleTarget()
            throws IdentityServiceException, TargetNotFoundException {
        final Map<Long, EntityDTO> entities = ImmutableMap.of(
            1L, virtualMachine("foo")
                .buying(vCpuMHz().from("bar").used(100.0))
                .buying(storageAmount().from("baz").used(200.0))
                .build(),
            2L, physicalMachine("bar").build(),
            3L, storage("baz").build());

        final Clock mockClock = Mockito.mock(Clock.class);
        Mockito.when(mockClock.millis()).thenReturn(12345L);
        entityStore = new EntityStore(targetStore, identityProvider, sender, mockClock);

        // Pretend that any target exists
        when(targetStore.getTarget(anyLong())).thenReturn(Optional.of(Mockito.mock(Target.class)));

        addEntities(entities);
        // the probe type doesn't matter here, just return any non-cloud probe type so it gets
        // treated as normal probe
        when(targetStore.getProbeTypeForTarget(Mockito.anyLong())).thenReturn(Optional.of(SDKProbeType.HYPERV));
        final TopologyStitchingGraph graph = entityStore.constructStitchingContext()
                .getStitchingGraph();

        final TopologyStitchingEntity foo = entityByLocalId(graph, "foo");
        assertEquals(12345L, foo.getLastUpdatedTime());
        assertThat(entityByLocalId(graph, "bar")
            .getConsumers().stream()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), contains("foo"));
        assertThat(entityByLocalId(graph, "baz")
            .getConsumers().stream()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), contains("foo"));
        assertThat(foo.getConsumers().stream()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), is(empty()));
        assertThat(foo.getProviders().stream()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), containsInAnyOrder("bar", "baz"));
    }

    /**
     * Tests construct stitching graph for multiple targets.
     *
     * @throws IdentityServiceException on service exceptions
     * @throws TargetNotFoundException if taret not found
     */
    @Test
    public void testConstructStitchingGraphMultipleTargets()
            throws IdentityServiceException, TargetNotFoundException {

        final long target1Id = 1234L;
        final long target2Id = 5678L;
        final Map<Long, EntityDTO> firstTargetEntities = ImmutableMap.of(
            1L, virtualMachine("foo")
                .buying(vCpuMHz().from("bar").used(100.0))
                .buying(storageAmount().from("baz").used(200.0))
                .build(),
            2L, physicalMachine("bar").build(),
            3L, storage("baz").build());
        final Map<Long, EntityDTO> secondTargetEntities = ImmutableMap.of(
            4L, virtualMachine("vampire")
                .buying(vCpuMHz().from("werewolf").used(100.0))
                .buying(storageAmount().from("dragon").used(200.0))
                .build(),
            5L, physicalMachine("werewolf").build(),
            6L, storage("dragon").build());

        // Pretend that any target exists
        when(targetStore.getTarget(anyLong())).thenReturn(Optional.of(Mockito.mock(Target.class)));

        addEntities(firstTargetEntities, target1Id, 0L, DiscoveryType.FULL, 0);
        addEntities(secondTargetEntities, target2Id, 1L, DiscoveryType.FULL, 1);
        // the probe type doesn't matter here, just return any non-cloud probe type so it gets
        // treated as normal probe
        when(targetStore.getProbeTypeForTarget(Mockito.anyLong())).thenReturn(Optional.of(SDKProbeType.HYPERV));
        final TopologyStitchingGraph graph = entityStore.constructStitchingContext()
                .getStitchingGraph();

        assertEquals(6, graph.entityCount());
        assertThat(entityByLocalId(graph, "foo")
            .getProviders().stream()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), containsInAnyOrder("bar", "baz"));
        assertThat(entityByLocalId(graph, "vampire")
            .getProviders().stream()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), containsInAnyOrder("werewolf", "dragon"));

        assertEquals(target1Id, entityByLocalId(graph, "foo").getTargetId());
        assertEquals(target2Id, entityByLocalId(graph, "werewolf").getTargetId());
    }

    /**
     * Tests construct stitching graph for single target.
     *
     * @throws IdentityServiceException on service exceptions
     * @throws TargetNotFoundException if taret not found
     */
    @Test
    public void testApplyIncrementalEntities()
            throws IdentityServiceException, TargetNotFoundException {
        final Clock mockClock = Mockito.mock(Clock.class);
        Mockito.when(mockClock.millis()).thenReturn(12345L);
        entityStore = new EntityStore(targetStore, identityProvider, sender, mockClock);
        // the probe type doesn't matter here, just return any non-cloud probe type so it gets
        // treated as normal probe
        when(targetStore.getProbeTypeForTarget(Mockito.anyLong())).thenReturn(Optional.of(SDKProbeType.HYPERV));
        // Pretend that any target exists
        when(targetStore.getTarget(anyLong())).thenReturn(Optional.of(Mockito.mock(Target.class)));

        // full discovery: maintenance is false
        final Map<Long, EntityDTO> fullEntities1 = ImmutableMap.of(
            1L, physicalMachine("host").maintenance(false).build());
        addEntities(fullEntities1, targetId, 0, DiscoveryType.FULL, 0);
        // incremental discovery: maintenance is true
        final Map<Long, EntityDTO> incrementalEntities = ImmutableMap.of(
            1L, physicalMachine("host").maintenance(true).build());
        addEntities(incrementalEntities, targetId, 0, DiscoveryType.INCREMENTAL, 1);

        // first broadcast
        final TopologyStitchingGraph graph = entityStore.constructStitchingContext()
            .getStitchingGraph();
        final TopologyStitchingEntity pm = entityByLocalId(graph, "host");
        // verify that host is in maintenance, due to the incremental discovery
        assertTrue(pm.getEntityBuilder().getMaintenance());

        // do second broadcast, the maintenance is still true
        final TopologyStitchingGraph graph2 = entityStore.constructStitchingContext()
            .getStitchingGraph();
        final TopologyStitchingEntity pm2 = entityByLocalId(graph2, "host");
        assertTrue(pm2.getEntityBuilder().getMaintenance());

        // another incremental discovery: maintenance is false
        final Map<Long, EntityDTO> incrementalEntities2 = ImmutableMap.of(
            1L, physicalMachine("host").maintenance(false).build());
        addEntities(incrementalEntities2, targetId, 0, DiscoveryType.INCREMENTAL, 2);
        // incremental result is cached
        assertEquals(2, entityStore.getIncrementalEntities(targetId).get()
            .getEntitiesInDiscoveryOrder(1L).size());

        // then a new full discovery (maintenance is true)
        final Map<Long, EntityDTO> fullEntities2 = ImmutableMap.of(
            1L, physicalMachine("host").maintenance(true).build());
        addEntities(fullEntities2, targetId, 0, DiscoveryType.FULL, 3);
        // verify that old incremental cache is cleared
        assertTrue(entityStore.getIncrementalEntities(targetId).get()
            .getEntitiesInDiscoveryOrder(1L).isEmpty());
        // full response should be used in third broadcast
        final TopologyStitchingGraph graph3 = entityStore.constructStitchingContext()
            .getStitchingGraph();
        final TopologyStitchingEntity pm3 = entityByLocalId(graph3, "host");
        assertTrue(pm3.getEntityBuilder().getMaintenance());
        assertTrue(entityStore.getIncrementalEntities(targetId).get()
            .getEntitiesInDiscoveryOrder(1L).isEmpty());
    }

    /**
     * Tests entities restored.
     *
     * @throws TargetNotFoundException if target not found
     */
    @Test
    public void testEntitiesRestored() throws TargetNotFoundException {
        final String id1 = "en-hypervisorTarget";
        final EntityDTO entity1 = EntityDTO.newBuilder().setEntityType(EntityType.VIRTUAL_MACHINE)
            .setId(id1).build();
        Map<Long, EntityDTO> entitiesMap = ImmutableMap.of(1L, entity1);

        // Pretend that any target exists
        when(targetStore.getTarget(anyLong())).thenReturn(Optional.of(Mockito.mock(Target.class)));

        entityStore.entitiesRestored(targetId, 5678L, entitiesMap);

        Assert.assertTrue(entityStore.getTargetEntityIdMap(targetId).isPresent());
        Assert.assertTrue(entityStore.getEntity(1L).isPresent());
        assertEquals(entitiesMap, entityStore.discoveredByTarget(targetId));
    }

    /**
     * Test that incremental discovery response for different targets are cached.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testSendEntitiesWithNewState() throws Exception {
        when(targetStore.getTarget(anyLong())).thenReturn(Optional.of(Mockito.mock(Target.class)));
        final long targetId1 = 2001;
        final long oid1 = 1L;
        final EntityDTO entity1 = EntityDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE)
            .setId("pm1")
            .build();
        Map<Long, EntityDTO> entitiesMap1 = ImmutableMap.of(oid1, entity1);

        final int messageId1 = 0;

        // target 1: first incremental discovery
        // before
        addEntities(entitiesMap1, targetId1, 0, DiscoveryType.INCREMENTAL, messageId1);
        verify(sender, times(1)).onEntitiesWithNewState(Mockito.any());
    }

    /**
     * Tests restoring multiple entities with same OIDs on different targets.
     *
     * @throws TargetNotFoundException if target not found
     */
    @Test
    public void testRestoreMultipleEntitiesSameOidDifferentTargets() throws TargetNotFoundException {
        final String sharedId = "en-hypervisorTarget";
        final long sharedOid = 1L;

        // Pretend that any target exists
        when(targetStore.getTarget(anyLong())).thenReturn(Optional.of(Mockito.mock(Target.class)));

        final EntityDTO entity1 = EntityDTO.newBuilder().setEntityType(EntityType.VIRTUAL_MACHINE)
            .setId(sharedId).build();
        Map<Long, EntityDTO> target1Map = ImmutableMap.of(sharedOid, entity1);
        entityStore.entitiesRestored(targetId, 5678L, target1Map);

        final long target2Id = 9137L;
        final EntityDTO entity2 = EntityDTO.newBuilder().setEntityType(EntityType.VIRTUAL_MACHINE)
            .setId(sharedId).build();
        Map<Long, EntityDTO> target2Map = ImmutableMap.of(sharedOid, entity2);
        entityStore.entitiesRestored(target2Id, 87942L, target2Map);

        Assert.assertTrue(entityStore.getTargetEntityIdMap(targetId).isPresent());
        Assert.assertTrue(entityStore.getTargetEntityIdMap(target2Id).isPresent());
        Assert.assertEquals(2, entityStore.getEntity(sharedOid).get().getPerTargetInfo().size());

        assertEquals(target1Map, entityStore.discoveredByTarget(targetId));
        assertEquals(target1Map, entityStore.discoveredByTarget(target2Id));
    }

    private TopologyStitchingEntity entityByLocalId(@Nonnull final TopologyStitchingGraph graph,
                                                    @Nonnull final String id) {
        return graph.entities()
            .filter(e -> e.getLocalId().equals(id))
            .findFirst()
            .get();
    }

    private void addEntities(@Nonnull final Map<Long, EntityDTO> entities)
            throws IdentityServiceException, TargetNotFoundException {
        addEntities(entities, targetId, 0, DiscoveryType.FULL, 0);
    }

    private void addEntities(@Nonnull final Map<Long, EntityDTO> entities, final long targetId,
                             final long probeId, DiscoveryType discoveryType, int messageId)
            throws IdentityServiceException, TargetNotFoundException {
        Mockito.when(identityProvider.getIdsForEntities(
            Mockito.eq(probeId), Mockito.eq(new ArrayList<>(entities.values()))))
            .thenReturn(entities);
        Mockito.when(identityProvider.generateTopologyId()).thenReturn(1L);
        entityStore.entitiesDiscovered(probeId, targetId, messageId, discoveryType, new ArrayList<>(entities.values()));
    }
}
