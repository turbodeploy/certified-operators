package com.vmturbo.topology.processor.entity;

import java.util.HashMap;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
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
    public void testNoCommodities() throws Exception {
        final Optional<EntityValidationFailure> error =
            entityValidator.validateSingleEntity(entityBuilder(), true);
        Assert.assertFalse(error.isPresent());
    }

    @Test
    public void testGoodCommodityBought() {
        final Optional<EntityValidationFailure> error =
            entityValidator.validateSingleEntity(
                entityBuilder().addCommoditiesBoughtFromProviders(
                    boughtFromProvider(goodCommodityBought())), true);
        Assert.assertFalse(error.isPresent());
    }

    @Test
    public void testBadCommodityBought() {
        final Optional<EntityValidationFailure> error =
            entityValidator.validateSingleEntity(
                entityBuilder().addCommoditiesBoughtFromProviders(
                    boughtFromProvider(badCommodityBought())), true);
        Assert.assertTrue(error.isPresent());
    }

    @Test
    public void testGoodCommoditySold() {
        final Optional<EntityValidationFailure> error = entityValidator.validateSingleEntity(
                entityBuilder().addCommoditySoldList(goodCommoditySold()), true);
        Assert.assertFalse(error.isPresent());
    }

    @Test
    public void testBadCommoditySold() {
        final Optional<EntityValidationFailure> error = entityValidator.validateSingleEntity(
            entityBuilder().addCommoditySoldList(badCommoditySold()), true);
        Assert.assertTrue(error.isPresent());
    }

    @Test
    public void testBadCommoditySoldAndBought() {
        final Optional<EntityValidationFailure> error = entityValidator.validateSingleEntity(
                entityBuilder()
                    .addCommoditySoldList(badCommoditySold())
                    .addCommoditiesBoughtFromProviders(boughtFromProvider(badCommodityBought())), true);
        Assert.assertTrue(error.isPresent());
    }

    @Test
    public void testReplaceSoldCapacity() {
        final TopologyEntityDTO.Builder teBuilder = entityBuilder()
            .addCommoditySoldList(badCommoditySold());
        entityValidator
            .processIllegalCommodityValues(teBuilder, Optional.empty(), buildGraph(teBuilder));
        Assert.assertFalse(teBuilder.getCommoditySoldList(0).hasCapacity());
        Assert.assertFalse(teBuilder.getAnalysisSettings().getControllable());
        Assert.assertTrue(teBuilder.getCommoditySoldList(0).getUsed() >= 0);
        Assert.assertFalse(teBuilder.getCommoditySoldList(0).hasPeak());
    }

    @Test
    public void testReplaceNaNSoldCapacity() {
        final TopologyEntityDTO.Builder teBuilder = entityBuilder()
            .addCommoditySoldList(naNCommoditySoldCapacity());
        entityValidator
            .processIllegalCommodityValues(teBuilder, Optional.empty(), buildGraph(teBuilder));
        Assert.assertFalse(teBuilder.getCommoditySoldList(0).hasCapacity());
        Assert.assertFalse(teBuilder.getAnalysisSettings().getControllable());
    }

    @Test
    public void testReplaceNaNSoldCapacityAndNanUsed() {
        final TopologyEntityDTO.Builder teBuilder = entityBuilder()
            .addCommoditySoldList(naNCommoditySoldCapacityAndUsed());
        entityValidator
            .processIllegalCommodityValues(teBuilder, Optional.empty(), buildGraph(teBuilder));
        Assert.assertFalse(teBuilder.getCommoditySoldList(0).hasCapacity());
        Assert.assertFalse(teBuilder.getAnalysisSettings().getControllable());
        Assert.assertTrue(teBuilder.getCommoditySoldList(0).getUsed() == 0);
    }

    @Test
    public void testReplaceSoldZeroCapacity() {
        final TopologyEntityDTO.Builder teBuilder = entityBuilder()
            .addCommoditySoldList(zeroCapacityCommoditySold());
        entityValidator
            .processIllegalCommodityValues(teBuilder, Optional.empty(), buildGraph(teBuilder));
        Assert.assertFalse(teBuilder.getCommoditySoldList(0).hasCapacity());
        Assert.assertFalse(teBuilder.getAnalysisSettings().getControllable());
    }

    @Test
    public void testNoReplaceSoldUnsetCapacity() {
        final TopologyEntityDTO.Builder teBuilder =
            entityBuilder().addCommoditySoldList(noCapacityCommoditySold());
        entityValidator
            .processIllegalCommodityValues(teBuilder, Optional.empty(), buildGraph(teBuilder));
        Assert.assertFalse(teBuilder.getCommoditySoldList(0).hasCapacity());
        Assert.assertFalse(teBuilder.getAnalysisSettings().getControllable());
    }

    @Test
    public void testReplaceBoughtUsed() {
        final TopologyEntityDTO.Builder teBuilder = entityBuilder()
            .addCommoditiesBoughtFromProviders(boughtFromProvider(badCommodityBought()));
        entityValidator
            .processIllegalCommodityValues(teBuilder, Optional.empty(), buildGraph(teBuilder));
        Assert.assertTrue(
            teBuilder.getCommoditiesBoughtFromProviders(0).getCommodityBought(0).getUsed() >= 0);
        Assert.assertFalse(
            teBuilder.getCommoditiesBoughtFromProviders(0).getCommodityBought(0).hasPeak());
    }

    @Test
    public void testReplaceNaNBoughtUsed() {
        final TopologyEntityDTO.Builder teBuilder = entityBuilder()
            .addCommoditiesBoughtFromProviders(boughtFromProvider(naNCommodityBought()));
        entityValidator
            .processIllegalCommodityValues(teBuilder, Optional.empty(), buildGraph(teBuilder));
        Assert.assertTrue(
            teBuilder.getCommoditiesBoughtFromProviders(0).getCommodityBought(0).getUsed() == 0);
        Assert.assertTrue(
            teBuilder.getCommoditiesBoughtFromProviders(0).getCommodityBought(0).getPeak() == 0);
    }

    /**
     * Test that we set controllable to false on the entity which has a comm sold with illegal
     * capacity and its consumers which buy this commodity.
     */
    @Test
    public void testControllableFalseWhenIllegalCapacity() {
        // VSAN PM
        TopologyEntity.Builder pm = TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .addCommoditySoldList(badCommoditySold())
            .setOid(100));

        // VM placed on PM
        TopologyEntity.Builder vm = TopologyEntity.newBuilder(entityBuilder()
            .setOid(1)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(
                    CommodityType.newBuilder().setType(CommodityDTO.CommodityType.CPU_VALUE).build()))
                .setProviderId(100)));

        // Storage placed on VSAN PM
        TopologyEntity.Builder storage = TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.STORAGE_VALUE)
            .setOid(2)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .addCommodityBought(badCommodityBought()).setProviderId(100)));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(
            ImmutableMap.of(
                pm.getOid(), pm,
                vm.getOid(), vm,
                storage.getOid(), storage));

        entityValidator
            .processIllegalCommodityValues(pm.getEntityBuilder(), Optional.empty(), graph);
        Assert.assertFalse(pm.getEntityBuilder().getAnalysisSettings().getControllable());
        Assert.assertTrue(vm.getEntityBuilder().getAnalysisSettings().getControllable());
        Assert.assertFalse(storage.getEntityBuilder().getAnalysisSettings().getControllable());
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
        final Builder result = te.getEntityBuilder();
        Assert.assertEquals(0,
            result.getCommoditiesBoughtFromProviders(0).getCommodityBought(0).getPeak(), 0);
        Assert.assertEquals(0,
            result.getCommoditiesBoughtFromProviders(0).getCommodityBought(0).getUsed(), 0);
        Assert.assertEquals(0, result.getCommoditySoldList(0).getPeak(), 0);
        Assert.assertEquals(0, result.getCommoditySoldList(0).getUsed(), 0);
        Assert.assertFalse(result.getCommoditySoldList(0).hasCapacity());
        Assert.assertFalse(result.getAnalysisSettings().getControllable());
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
        final Builder result = te.getEntityBuilder();
        Assert.assertEquals(0, result.getCommoditySoldList(0).getPeak(), 0);
        Assert.assertEquals(0, result.getCommoditySoldList(0).getUsed(), 0);
        Assert.assertFalse(result.getCommoditySoldList(0).hasCapacity());
        Assert.assertFalse(result.getAnalysisSettings().getControllable());
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
        final Builder result = te2.getEntityBuilder();
        Assert.assertEquals(0, result.getCommoditySoldList(0).getPeak(), 0);
        Assert.assertEquals(0, result.getCommoditySoldList(0).getUsed(), 0);
        Assert.assertTrue(result.getCommoditySoldList(0).hasCapacity());
        Assert.assertTrue(result.getAnalysisSettings().getControllable());
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
                        .setClonedFromEntity(te1.getEntityBuilder());
        ev.validateTopologyEntities(buildGraph(te2), true);
        final Builder result = te2.getEntityBuilder();
        Assert.assertEquals(0, result.getCommoditySoldList(0).getPeak(), 0);
        Assert.assertEquals(0, result.getCommoditySoldList(0).getUsed(), 0);
        Assert.assertTrue(result.getCommoditySoldList(0).hasCapacity());
        Assert.assertTrue(result.getAnalysisSettings().getControllable());
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
        final Builder result = te2.getEntityBuilder();
        Assert.assertEquals(0, result.getCommoditySoldList(0).getPeak(), 0);
        Assert.assertEquals(0, result.getCommoditySoldList(0).getUsed(), 0);
        Assert.assertFalse(result.getCommoditySoldList(0).hasCapacity());
        Assert.assertFalse(result.getAnalysisSettings().getControllable());
    }

    private TopologyEntityDTO.Builder entityBuilder() {
        return TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setDisplayName("ASDF");
    }

    private CommoditiesBoughtFromProvider boughtFromProvider(
        @Nonnull final CommodityBoughtDTO bought) {
        return CommoditiesBoughtFromProvider.newBuilder()
            .setProviderId(123456789)
            .addCommodityBought(bought)
            .build();
    }

    private CommodityBoughtDTO goodCommodityBought() {
        return CommodityBoughtDTO.newBuilder().setCommodityType(
            TopologyDTO.CommodityType.newBuilder().setType(1)).setPeak(1).setUsed(1).build();
    }

    private CommodityBoughtDTO badCommodityBought() {
        return CommodityBoughtDTO.newBuilder().setCommodityType(
            TopologyDTO.CommodityType.newBuilder().setType(1)).setPeak(-1).setUsed(-1).build();
    }

    private CommodityBoughtDTO naNCommodityBought() {
        return CommodityBoughtDTO.newBuilder().setCommodityType(
            TopologyDTO.CommodityType.newBuilder().setType(1)).setPeak(Double.NaN)
                        .setUsed(Double.NaN).build();
    }

    private CommoditySoldDTO goodCommoditySold() {
        return CommoditySoldDTO.newBuilder().setCommodityType(
            TopologyDTO.CommodityType.newBuilder().setType(1)).setCapacity(4).setPeak(4).setUsed(4)
            .build();
    }

    private CommoditySoldDTO badCommoditySold() {
        return CommoditySoldDTO.newBuilder().setCommodityType(
            TopologyDTO.CommodityType.newBuilder().setType(1)).setCapacity(-4).setPeak(-4)
            .setUsed(-4).build();
    }

    private CommoditySoldDTO naNCommoditySoldCapacity() {
        return CommoditySoldDTO.newBuilder().setCommodityType(
            TopologyDTO.CommodityType.newBuilder().setType(1)).setCapacity(Double.NaN)
            .setUsed(1).build();
    }

    private CommoditySoldDTO naNCommoditySoldCapacityAndUsed() {
        return CommoditySoldDTO.newBuilder().setCommodityType(
            TopologyDTO.CommodityType.newBuilder().setType(1)).setCapacity(Double.NaN)
            .setUsed(Double.NaN).build();
    }

    private CommoditySoldDTO zeroCapacityCommoditySold() {
        return CommoditySoldDTO.newBuilder().setCommodityType(
            TopologyDTO.CommodityType.newBuilder().setType(1)).setCapacity(0).setPeak(0).setUsed(0)
            .build();
    }

    private CommoditySoldDTO noCapacityCommoditySold() {
        return CommoditySoldDTO.newBuilder().setCommodityType(
            TopologyDTO.CommodityType.newBuilder().setType(1)).setPeak(0).setUsed(0).build();
    }

    private TopologyGraph<TopologyEntity> buildGraph(TopologyEntity.Builder te) {
        return TopologyEntityTopologyGraphCreator.newGraph(ImmutableMap.of(te.getOid(), te));
    }

    private TopologyGraph<TopologyEntity> buildGraph(TopologyEntityDTO.Builder dto) {
        return buildGraph(TopologyEntity.newBuilder(dto));
    }
}
