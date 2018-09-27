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

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.topology.processor.identity.IdentityMetadataMissingException;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderException;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.stitching.TopologyStitchingGraph;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.TargetStoreListener;

/**
 * Test the {@link EntityStore} methods.
 */
public class EntityStoreTest {
    private final long targetId = 1234;

    private TargetStore targetStore = Mockito.mock(TargetStore.class);

    private IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);

    private EntityStore entityStore = new EntityStore(targetStore, identityProvider,
        Clock.systemUTC());

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
        addEntities(entitiesMap);

        Assert.assertTrue(entityStore.getTargetEntityIdMap(targetId).isPresent());
        Assert.assertTrue(entityStore.getEntity(1L).isPresent());
        Assert.assertTrue(entityStore.discoveredByTarget(targetId).equals(entitiesMap));
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

    @Test
    public void testConstructStitchingGraphSingleTarget()
        throws EntitiesValidationException, IdentityUninitializedException,
            IdentityMetadataMissingException, IdentityProviderException {
        final Map<Long, EntityDTO> entities = ImmutableMap.of(
            1L, virtualMachine("foo")
                .buying(vCpuMHz().from("bar").used(100.0))
                .buying(storageAmount().from("baz").used(200.0))
                .build(),
            2L, physicalMachine("bar").build(),
            3L, storage("baz").build());

        final Clock mockClock = Mockito.mock(Clock.class);
        Mockito.when(mockClock.millis()).thenReturn(12345L);
        entityStore = new EntityStore(targetStore, identityProvider, mockClock);
        addEntities(entities);
        // return non-cloud probe types so it gets treated as normal probes
        Mockito.when(targetStore.getProbeTypeForTarget(Mockito.anyLong())).thenReturn(
                Optional.of(SDKProbeType.VCENTER));
        final TopologyStitchingGraph graph = entityStore.constructStitchingContext(targetStore,
                Collections.emptyMap()).getStitchingGraph();

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

    @Test
    public void testConstructStitchingGraphMultipleTargets()
        throws EntitiesValidationException, IdentityUninitializedException,
                IdentityMetadataMissingException, IdentityProviderException {

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

        addEntities(firstTargetEntities, target1Id, 0L);
        addEntities(secondTargetEntities, target2Id, 1L);

        // return non-cloud probe types so it gets treated as normal probes
        Mockito.when(targetStore.getProbeTypeForTarget(Mockito.anyLong())).thenReturn(
                Optional.of(SDKProbeType.VCENTER));
        final TopologyStitchingGraph graph = entityStore.constructStitchingContext(targetStore,
                Collections.emptyMap()).getStitchingGraph();

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

    @Test
    public void testEntitiesRestored() {
        final String id1 = "en-hypervisorTarget";
        final EntityDTO entity1 = EntityDTO.newBuilder().setEntityType(EntityType.VIRTUAL_MACHINE)
            .setId(id1).build();
        Map<Long, EntityDTO> entitiesMap = ImmutableMap.of(1L, entity1);
        entityStore.entitiesRestored(targetId, 5678L, entitiesMap);

        Assert.assertTrue(entityStore.getTargetEntityIdMap(targetId).isPresent());
        Assert.assertTrue(entityStore.getEntity(1L).isPresent());
        Assert.assertTrue(entityStore.discoveredByTarget(targetId).equals(entitiesMap));
    }

    @Test
    public void testRestoreMultipleEntitiesSameOidDifferentTargets() {
        final String sharedId = "en-hypervisorTarget";
        final long sharedOid = 1L;

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

        Assert.assertTrue(entityStore.discoveredByTarget(targetId).equals(target1Map));
        Assert.assertTrue(entityStore.discoveredByTarget(target2Id).equals(target1Map));
    }

    private TopologyStitchingEntity entityByLocalId(@Nonnull final TopologyStitchingGraph graph,
                                                    @Nonnull final String id) {
        return graph.entities()
            .filter(e -> e.getLocalId().equals(id))
            .findFirst()
            .get();
    }

    private void addEntities(@Nonnull final Map<Long, EntityDTO> entities)
            throws EntitiesValidationException, IdentityUninitializedException,
                    IdentityMetadataMissingException, IdentityProviderException {
        addEntities(entities, targetId, 0);
    }

    private void addEntities(@Nonnull final Map<Long, EntityDTO> entities, final long targetId,
                             final long probeId)
        throws EntitiesValidationException, IdentityUninitializedException,
                    IdentityMetadataMissingException, IdentityProviderException {
        Mockito.when(identityProvider.getIdsForEntities(
            Mockito.eq(probeId), Mockito.eq(new ArrayList<>(entities.values()))))
            .thenReturn(entities);
        entityStore.entitiesDiscovered(probeId, targetId,
                new ArrayList<>(entities.values()));
    }
}
