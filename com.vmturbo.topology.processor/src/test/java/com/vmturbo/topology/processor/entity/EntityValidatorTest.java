package com.vmturbo.topology.processor.entity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderImpl;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.entity.EntityValidator.EntityValidationFailure;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

/**
 * Unit tests to make sure EntityValidator returns expected information.
 */
public class EntityValidatorTest {

    private final EntityValidator entityValidator = new EntityValidator(false);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testNoCommodities() {
        final Optional<EntityValidationFailure> error =
            entityValidator.validateSingleEntity(entityBuilder(), true);
        assertFalse(error.isPresent());
    }

    @Test
    public void testGoodCommodityBought() {
        final Optional<EntityValidationFailure> error =
            entityValidator.validateSingleEntity(
                entityBuilder().addCommoditiesBoughtFromProviders(
                    boughtFromProvider(goodCommodityBought())), true);
        assertFalse(error.isPresent());
    }

    @Test
    public void testBadCommodityBought() {
        final Optional<EntityValidationFailure> error =
            entityValidator.validateSingleEntity(
                entityBuilder().addCommoditiesBoughtFromProviders(
                    boughtFromProvider(badCommodityBought())), true);
        assertTrue(error.isPresent());
    }

    @Test
    public void testGoodCommoditySold() {
        final Optional<EntityValidationFailure> error = entityValidator.validateSingleEntity(
                entityBuilder().addCommoditySoldList(goodCommoditySold()), true);
        assertFalse(error.isPresent());
    }

    @Test
    public void testBadCommoditySold() {
        final Optional<EntityValidationFailure> error = entityValidator.validateSingleEntity(
            entityBuilder().addCommoditySoldList(badCommoditySold()), true);
        assertTrue(error.isPresent());
    }

    @Test
    public void testBadCommoditySoldAndBought() {
        final Optional<EntityValidationFailure> error = entityValidator.validateSingleEntity(
                entityBuilder()
                    .addCommoditySoldList(badCommoditySold())
                    .addCommoditiesBoughtFromProviders(boughtFromProvider(badCommodityBought())), true);
        assertTrue(error.isPresent());
    }

    @Test
    public void testReplaceSoldCapacity() {
        final TopologyEntityImpl teBuilder = entityBuilder()
            .addCommoditySoldList(badCommoditySold());
        entityValidator
            .processIllegalCommodityValues(teBuilder, Optional.empty(), buildGraph(teBuilder));
        assertFalse(teBuilder.getCommoditySoldList(0).hasCapacity());
        assertFalse(teBuilder.getAnalysisSettings().getControllable());
        assertTrue(teBuilder.getCommoditySoldList(0).getUsed() >= 0);
        assertFalse(teBuilder.getCommoditySoldList(0).hasPeak());
    }

    @Test
    public void testReplaceNaNSoldCapacity() {
        final TopologyEntityImpl teBuilder = entityBuilder()
            .addCommoditySoldList(naNCommoditySoldCapacity());
        entityValidator
            .processIllegalCommodityValues(teBuilder, Optional.empty(), buildGraph(teBuilder));
        assertFalse(teBuilder.getCommoditySoldList(0).hasCapacity());
        assertFalse(teBuilder.getAnalysisSettings().getControllable());
    }

    @Test
    public void testReplaceNaNSoldCapacityAndNanUsed() {
        final TopologyEntityImpl teBuilder = entityBuilder()
            .addCommoditySoldList(naNCommoditySoldCapacityAndUsed());
        entityValidator
            .processIllegalCommodityValues(teBuilder, Optional.empty(), buildGraph(teBuilder));
        assertFalse(teBuilder.getCommoditySoldList(0).hasCapacity());
        assertFalse(teBuilder.getAnalysisSettings().getControllable());
        assertEquals(0, teBuilder.getCommoditySoldList(0).getUsed(), 0.0);
    }

    @Test
    public void testReplaceSoldZeroCapacity() {
        final TopologyEntityImpl teBuilder = entityBuilder()
            .addCommoditySoldList(zeroCapacityCommoditySold());
        entityValidator
            .processIllegalCommodityValues(teBuilder, Optional.empty(), buildGraph(teBuilder));
        assertFalse(teBuilder.getCommoditySoldList(0).hasCapacity());
        assertFalse(teBuilder.getAnalysisSettings().getControllable());
    }

    @Test
    public void testNoReplaceSoldUnsetCapacity() {
        final TopologyEntityImpl teBuilder =
            entityBuilder().addCommoditySoldList(noCapacityCommoditySold());
        entityValidator
            .processIllegalCommodityValues(teBuilder, Optional.empty(), buildGraph(teBuilder));
        assertFalse(teBuilder.getCommoditySoldList(0).hasCapacity());
        assertFalse(teBuilder.getAnalysisSettings().getControllable());
    }

    @Test
    public void testReplaceBoughtUsed() {
        final TopologyEntityImpl teBuilder = entityBuilder()
            .addCommoditiesBoughtFromProviders(boughtFromProvider(badCommodityBought()));
        entityValidator
            .processIllegalCommodityValues(teBuilder, Optional.empty(), buildGraph(teBuilder));
        assertTrue(
            teBuilder.getCommoditiesBoughtFromProviders(0).getCommodityBought(0).getUsed() >= 0);
        assertFalse(
            teBuilder.getCommoditiesBoughtFromProviders(0).getCommodityBought(0).hasPeak());
    }

    @Test
    public void testReplaceNaNBoughtUsed() {
        final TopologyEntityImpl teBuilder = entityBuilder()
            .addCommoditiesBoughtFromProviders(boughtFromProvider(naNCommodityBought()));
        final Optional<EntityValidationFailure> failure = entityValidator
            .validateSingleEntity(teBuilder, false);
        assertTrue(failure.isPresent());
        assertTrue(failure.get().toString().contains("NaN"));
        entityValidator
            .processIllegalCommodityValues(teBuilder, Optional.empty(), buildGraph(teBuilder));
        assertEquals(0, teBuilder.getCommoditiesBoughtFromProviders(0).getCommodityBought(0).getUsed(), 0.0);
        assertEquals(0, teBuilder.getCommoditiesBoughtFromProviders(0).getCommodityBought(0).getPeak(), 0.0);
    }

    /**
     * Test that we set controllable to false on the entity which has a comm sold with illegal
     * capacity and its consumers which buy this commodity.
     */
    @Test
    public void testControllableFalseWhenIllegalCapacity() {
        // VSAN PM
        TopologyEntity.Builder pm = TopologyEntity.newBuilder(new TopologyEntityImpl()
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .addCommoditySoldList(badCommoditySold())
            .setOid(100));

        // VM placed on PM
        TopologyEntity.Builder vm = TopologyEntity.newBuilder(entityBuilder()
            .setOid(1)
            .addCommoditiesBoughtFromProviders(new CommoditiesBoughtFromProviderImpl()
                .addCommodityBought(new CommodityBoughtImpl().setCommodityType(
                    new CommodityTypeImpl().setType(CommodityDTO.CommodityType.CPU_VALUE)))
                .setProviderId(100)));

        // Storage placed on VSAN PM
        TopologyEntity.Builder storage = TopologyEntity.newBuilder(new TopologyEntityImpl()
            .setEntityType(EntityType.STORAGE_VALUE)
            .setOid(2)
            .addCommoditiesBoughtFromProviders(new CommoditiesBoughtFromProviderImpl()
                .addCommodityBought(badCommodityBought()).setProviderId(100)));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(
            ImmutableMap.of(
                pm.getOid(), pm,
                vm.getOid(), vm,
                storage.getOid(), storage));

        entityValidator
            .processIllegalCommodityValues(pm.getTopologyEntityImpl(), Optional.empty(), graph);
        assertFalse(pm.getTopologyEntityImpl().getAnalysisSettings().getControllable());
        assertTrue(vm.getTopologyEntityImpl().getAnalysisSettings().getControllable());
        assertFalse(storage.getTopologyEntityImpl().getAnalysisSettings().getControllable());
    }

    @Test
    public void testEmpty() throws Exception {
        entityValidator.validateTopologyEntities(TopologyEntityTopologyGraphCreator.newGraph(new HashMap<>()), false);
        //no exception thrown
    }

    @Test
    public void testEntitiesWithNoCommodities() throws EntitiesValidationException {
        entityValidator.validateTopologyEntities(
            buildGraph(TopologyEntity.newBuilder(entityBuilder())), false);
        //no exception thrown
    }

    @Test
    public void testEntitiesWithGoodCommodities() throws EntitiesValidationException {
        entityValidator.validateTopologyEntities(buildGraph(TopologyEntity.newBuilder(entityBuilder()
            .addCommoditiesBoughtFromProviders(boughtFromProvider(goodCommodityBought()))
            .addCommoditySoldList(goodCommoditySold()))), false);
        //no exception thrown
    }

    @Test
    public void testEntitiesWithBadCommodities() throws EntitiesValidationException {
        final TopologyEntity.Builder te = TopologyEntity.newBuilder(entityBuilder()
            .addCommoditiesBoughtFromProviders(boughtFromProvider(badCommodityBought()))
            .addCommoditySoldList(badCommoditySold()));
        entityValidator.validateTopologyEntities(buildGraph(te), false);
        final TopologyEntityImpl result = te.getTopologyEntityImpl();
        assertEquals(0,
            result.getCommoditiesBoughtFromProviders(0).getCommodityBought(0).getPeak(), 0);
        assertEquals(0,
            result.getCommoditiesBoughtFromProviders(0).getCommodityBought(0).getUsed(), 0);
        assertEquals(0, result.getCommoditySoldList(0).getPeak(), 0);
        assertEquals(0, result.getCommoditySoldList(0).getUsed(), 0);
        assertFalse(result.getCommoditySoldList(0).hasCapacity());
        assertFalse(result.getAnalysisSettings().getControllable());
    }

    /**
     * Ensure that without cached capacity entry the bad capacity gets unset.
     *
     * @throws EntitiesValidationException when failed
     */
    @Test
    public void testCapacityCacheEnabledEntryAbsent() throws EntitiesValidationException {
        EntityValidator ev = new EntityValidator(true);
        final TopologyEntity.Builder te = TopologyEntity.newBuilder(entityBuilder()
            .addCommoditySoldList(badCommoditySold()));
        ev.validateTopologyEntities(buildGraph(te), false);
        final TopologyEntityImpl result = te.getTopologyEntityImpl();
        assertEquals(0, result.getCommoditySoldList(0).getPeak(), 0);
        assertEquals(0, result.getCommoditySoldList(0).getUsed(), 0);
        assertFalse(result.getCommoditySoldList(0).hasCapacity());
        assertFalse(result.getAnalysisSettings().getControllable());
    }

    /**
     * Ensure that good entity creates a capacity cache entry which gets picked for the
     * next bad live entity validation.
     *
     * @throws EntitiesValidationException when failed
     */
    @Test
    public void testCapacityCacheEnabledEntryPresent() throws EntitiesValidationException {
        EntityValidator ev = new EntityValidator(true);
        final TopologyEntity.Builder te1 = TopologyEntity.newBuilder(entityBuilder()
            .addCommoditySoldList(goodCommoditySold()));
        ev.validateTopologyEntities(buildGraph(te1), false);
        final TopologyEntity.Builder te2 = TopologyEntity.newBuilder(entityBuilder()
                        .addCommoditySoldList(badCommoditySold()));
        ev.validateTopologyEntities(buildGraph(te2), false);
        final TopologyEntityImpl result = te2.getTopologyEntityImpl();
        assertEquals(0, result.getCommoditySoldList(0).getPeak(), 0);
        assertEquals(0, result.getCommoditySoldList(0).getUsed(), 0);
        assertTrue(result.getCommoditySoldList(0).hasCapacity());
        assertTrue(result.getAnalysisSettings().getControllable());
    }

    /**
     * Ensure that good entity creates a capacity cache entry which gets picked for the
     * next bad plan entity validation.
     *
     * @throws EntitiesValidationException when failed
     */
    @Test
    public void testCapacityCacheEnabledEntryPresentInPlan() throws EntitiesValidationException {
        EntityValidator ev = new EntityValidator(true);
        final TopologyEntity.Builder te1 = TopologyEntity.newBuilder(entityBuilder()
            .addCommoditySoldList(goodCommoditySold()));
        ev.validateTopologyEntities(buildGraph(te1), false);
        final TopologyEntity.Builder te2 = TopologyEntity.newBuilder(entityBuilder()
                        .addCommoditySoldList(badCommoditySold()))
                        .setClonedFromEntity(te1.getTopologyEntityImpl());
        ev.validateTopologyEntities(buildGraph(te2), true);
        final TopologyEntityImpl result = te2.getTopologyEntityImpl();
        assertEquals(0, result.getCommoditySoldList(0).getPeak(), 0);
        assertEquals(0, result.getCommoditySoldList(0).getUsed(), 0);
        assertTrue(result.getCommoditySoldList(0).hasCapacity());
        assertTrue(result.getAnalysisSettings().getControllable());
    }

    /**
     * Ensure that plan topology validation does not affect capacity cache.
     *
     * @throws EntitiesValidationException when failed
     */
    @Test
    public void testPlanValidationNoCacheUpdate() throws EntitiesValidationException {
        EntityValidator ev = new EntityValidator(true);
        final TopologyEntity.Builder te1 = TopologyEntity.newBuilder(entityBuilder()
            .addCommoditySoldList(goodCommoditySold()));
        ev.validateTopologyEntities(buildGraph(te1), true);
        final TopologyEntity.Builder te2 = TopologyEntity.newBuilder(entityBuilder()
                        .addCommoditySoldList(badCommoditySold()));
        ev.validateTopologyEntities(buildGraph(te2), false);
        final TopologyEntityImpl result = te2.getTopologyEntityImpl();
        assertEquals(0, result.getCommoditySoldList(0).getPeak(), 0);
        assertEquals(0, result.getCommoditySoldList(0).getUsed(), 0);
        assertFalse(result.getCommoditySoldList(0).hasCapacity());
        assertFalse(result.getAnalysisSettings().getControllable());
    }

    private TopologyEntityImpl entityBuilder() {
        return new TopologyEntityImpl()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setDisplayName("ASDF");
    }

    private CommoditiesBoughtFromProviderImpl boughtFromProvider(
        @Nonnull final CommodityBoughtImpl bought) {
        return new CommoditiesBoughtFromProviderImpl()
            .setProviderId(123456789)
            .addCommodityBought(bought);
    }

    private CommodityBoughtImpl goodCommodityBought() {
        return new CommodityBoughtImpl().setCommodityType(
            new CommodityTypeImpl().setType(1)).setPeak(1).setUsed(1);
    }

    private CommodityBoughtImpl badCommodityBought() {
        return new CommodityBoughtImpl().setCommodityType(
                new CommodityTypeImpl().setType(1)).setPeak(-1).setUsed(-1);
    }

    private CommodityBoughtImpl naNCommodityBought() {
        return new CommodityBoughtImpl().setCommodityType(
                        new CommodityTypeImpl().setType(1)).setPeak(Double.NaN)
                        .setUsed(Double.NaN);
    }

    private CommoditySoldImpl goodCommoditySold() {
        return new CommoditySoldImpl().setCommodityType(
                        new CommodityTypeImpl().setType(1)).setCapacity(4).setPeak(4).setUsed(4);
    }

    private CommoditySoldImpl badCommoditySold() {
        return new CommoditySoldImpl().setCommodityType(
                        new CommodityTypeImpl().setType(1)).setCapacity(-4).setPeak(-4)
            .setUsed(-4);
    }

    private CommoditySoldImpl naNCommoditySoldCapacity() {
        return new CommoditySoldImpl().setCommodityType(
                        new CommodityTypeImpl().setType(1)).setCapacity(Double.NaN)
            .setUsed(1);
    }

    private CommoditySoldImpl naNCommoditySoldCapacityAndUsed() {
        return new CommoditySoldImpl().setCommodityType(
                        new CommodityTypeImpl().setType(1)).setCapacity(Double.NaN)
            .setUsed(Double.NaN);
    }

    private CommoditySoldImpl zeroCapacityCommoditySold() {
        return new CommoditySoldImpl().setCommodityType(
                        new CommodityTypeImpl().setType(1)).setCapacity(0).setPeak(0).setUsed(0);
    }

    private CommoditySoldImpl noCapacityCommoditySold() {
        return new CommoditySoldImpl().setCommodityType(
                new CommodityTypeImpl().setType(1)).setPeak(0).setUsed(0);
    }

    private TopologyGraph<TopologyEntity> buildGraph(TopologyEntity.Builder te) {
        return TopologyEntityTopologyGraphCreator.newGraph(ImmutableMap.of(te.getOid(), te));
    }

    private TopologyGraph<TopologyEntity> buildGraph(TopologyEntityImpl dto) {
        return buildGraph(TopologyEntity.newBuilder(dto));
    }
}
