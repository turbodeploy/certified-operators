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
import java.util.List;
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

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.topology.processor.entity.EntityValidator.EntityValidationFailure;
import com.vmturbo.topology.processor.identity.IdentityProvider;
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

    private EntityValidator entityValidator = Mockito.spy(new EntityValidator());

    private EntityStore entityStore = new EntityStore(targetStore, identityProvider, entityValidator,
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
        final String id1 = "en-target1";
        final EntityDTO entity1 = EntityDTO.newBuilder().setEntityType(EntityType.VIRTUAL_MACHINE)
                .setId(id1).build();
        Map<Long, EntityDTO> entitiesMap = ImmutableMap.of(1L, entity1);
        addEntities(entitiesMap);

        Assert.assertTrue(entityStore.getTargetEntityIdMap(targetId).isPresent());
        Assert.assertTrue(entityStore.getEntity(1L).isPresent());
        Assert.assertTrue(entityStore.discoveredByTarget(targetId).equals(entitiesMap));
    }

    /**
     * Test that {@link EntityStore#entitiesDiscovered(long, long, List)} throws
     * an exception when entities fail to validate.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testEntitiesValidationFailure() throws Exception {
        final long entityId = 1;
        final String id1 = "en-target1";
        final EntityDTO entity1 = EntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .setId(id1)
                .build();

        // Force an error!
        final EntityValidationFailure validationFailure =
                        new EntityValidationFailure(entityId, entity1, "ERROR");
        Mockito.doReturn(Optional.of(validationFailure))
                .when(entityValidator).validateEntityDTO(Mockito.anyLong(), Mockito.any());
        Mockito.doReturn(Optional.of(validationFailure))
            .when(entityValidator).validateEntityDTO(
                Mockito.anyLong(), Mockito.any(), Mockito.anyBoolean());

        expectedException.expect(EntitiesValidationException.class);

        addEntities(ImmutableMap.of(1L, entity1));
    }

    /**
     * Test that {@link EntityStore#entitiesDiscovered(long, long, List)} doesn't
     * replace illegal commodity values in entities that DON'T fail validation.
     * This is important, because we don't want to do all the reconstruction when
     * the entity is valid.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testEntitiesNoFailureNoReplaceIllegalCommodities() throws Exception {
        final String id1 = "en-target1";
        final CommodityDTO boughtCommodity = CommodityDTO.newBuilder()
                .setKey("bought")
                .setCommodityType(CommodityType.CPU)
                .build();
        final CommodityDTO soldCommodity = CommodityDTO.newBuilder()
                .setKey("sold")
                .setCommodityType(CommodityType.VCPU)
                .build();
        final EntityDTO entity1 = EntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                // We don't care if the commodities actually have illegal
                // values, since we force the validation failure via mockito.
                .addCommoditiesSold(soldCommodity)
                .addCommoditiesBought(CommodityBought.newBuilder()
                        .setProviderId("blah")
                        .addBought(boughtCommodity))
                .setId(id1)
                .build();

        // No validation failures.
        Mockito.doReturn(Optional.empty())
                .when(entityValidator).validateEntityDTO(Mockito.anyLong(), Mockito.any());

        addEntities(ImmutableMap.of(1L, entity1));

        Mockito.verify(entityValidator, Mockito.times(0)).replaceIllegalCommodityValues(
                Mockito.any(), Mockito.any(), Mockito.anyBoolean());

    }

    /**
     * Test that {@link EntityStore#entitiesDiscovered(long, long, List)} replaces
     * illegal commodity values in entities that fail validation.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testEntitiesReplaceIllegalCommodityValues() throws Exception {
        final long entityId = 1;
        final String id1 = "en-target1";
        final CommodityDTO boughtCommodity = CommodityDTO.newBuilder()
                .setKey("bought")
                .setCommodityType(CommodityType.CPU)
                .build();
        final CommodityDTO soldCommodity = CommodityDTO.newBuilder()
                .setKey("sold")
                .setCommodityType(CommodityType.VCPU)
                .build();
        final EntityDTO entity1 = EntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                // We don't care if the commodities actually have illegal
                // values, since we force the validation failure via mockito.
                .addCommoditiesSold(soldCommodity)
                .addCommoditiesBought(CommodityBought.newBuilder()
                    .setProviderId("blah")
                    .addBought(boughtCommodity))
                .setId(id1)
                .build();

        // Force an error!
        final EntityValidationFailure validationFailure =
                        new EntityValidationFailure(entityId, entity1, "ERROR");
        Mockito.doReturn(Optional.of(validationFailure))
                .when(entityValidator).validateEntityDTO(Mockito.anyLong(), Mockito.any());
        Mockito.doReturn(Optional.of(validationFailure))
            .when(entityValidator).validateEntityDTO(
                Mockito.anyLong(), Mockito.any(), Mockito.anyBoolean());

        // Not using expectedException because that stops execution,
        // and we want to verify the calls.
        try {
            addEntities(ImmutableMap.of(1L, entity1));
            Assert.fail("Expected validation exception!");
        } catch (EntitiesValidationException e) {
            // Expected.
        }

        Mockito.verify(entityValidator).replaceIllegalCommodityValues(
            Mockito.eq(entity1), Mockito.eq(soldCommodity), Mockito.eq(true));
        Mockito.verify(entityValidator).replaceIllegalCommodityValues(
            Mockito.eq(entity1), Mockito.eq(boughtCommodity), Mockito.eq(false));
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
        final String id1 = "en-target1";
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
        throws EntitiesValidationException, IdentityUninitializedException {
        final Map<Long, EntityDTO> entities = ImmutableMap.of(
            1L, virtualMachine("foo")
                .buying(vCpuMHz().from("bar").used(100.0))
                .buying(storageAmount().from("baz").used(200.0))
                .build(),
            2L, physicalMachine("bar").build(),
            3L, storage("baz").build());

        final Clock mockClock = Mockito.mock(Clock.class);
        Mockito.when(mockClock.millis()).thenReturn(12345L);

        entityStore = new EntityStore(targetStore, identityProvider, entityValidator,
            mockClock);
        addEntities(entities);
        final TopologyStitchingGraph graph = entityStore
            .constructStitchingContext()
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

    @Test
    public void testConstructStitchingGraphMultipleTargets()
        throws EntitiesValidationException, IdentityUninitializedException {

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
        final TopologyStitchingGraph graph = entityStore
            .constructStitchingContext()
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

    private TopologyStitchingEntity entityByLocalId(@Nonnull final TopologyStitchingGraph graph,
                                                    @Nonnull final String id) {
        return graph.entities()
            .filter(e -> e.getLocalId().equals(id))
            .findFirst()
            .get();
    }

    private void addEntities(@Nonnull final Map<Long, EntityDTO> entities)
            throws EntitiesValidationException, IdentityUninitializedException {
        addEntities(entities, targetId, 0);
    }

    private void addEntities(@Nonnull final Map<Long, EntityDTO> entities, final long targetId,
                             final long probeId)
        throws EntitiesValidationException, IdentityUninitializedException {
        Mockito.when(identityProvider.getIdsForEntities(
            Mockito.eq(probeId), Mockito.eq(new ArrayList<>(entities.values()))))
            .thenReturn(entities);
        entityStore.entitiesDiscovered(probeId, targetId, new ArrayList<>(entities.values()));
    }
}
