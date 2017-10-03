package com.vmturbo.topology.processor.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
import com.vmturbo.topology.processor.entity.EntityValidator.EntityValidationFailure;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
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

    private EntityStore entityStore = new EntityStore(targetStore, identityProvider, entityValidator);

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

    private void addEntities(@Nonnull final Map<Long, EntityDTO> entities)
            throws EntitiesValidationException, IdentityUninitializedException {
        final long probeId = 0;
        Mockito.when(identityProvider.getIdsForEntities(
                Mockito.eq(probeId), Mockito.eq(new ArrayList<>(entities.values()))))
                .thenReturn(entities);
        entityStore.entitiesDiscovered(probeId, targetId, new ArrayList<>(entities.values()));
    }
}
