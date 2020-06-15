package com.vmturbo.topology.processor.entity;

import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.entity.EntityValidator.EntityValidationFailure;

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
            .processIllegalCommodityValues(teBuilder, Optional.empty());
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
            .processIllegalCommodityValues(teBuilder, Optional.empty());
        Assert.assertFalse(teBuilder.getCommoditySoldList(0).hasCapacity());
        Assert.assertFalse(teBuilder.getAnalysisSettings().getControllable());
    }

    @Test
    public void testReplaceNaNSoldCapacityAndNanUsed() {
        final TopologyEntityDTO.Builder teBuilder = entityBuilder()
            .addCommoditySoldList(naNCommoditySoldCapacityAndUsed());
        entityValidator
            .processIllegalCommodityValues(teBuilder, Optional.empty());
        Assert.assertFalse(teBuilder.getCommoditySoldList(0).hasCapacity());
        Assert.assertFalse(teBuilder.getAnalysisSettings().getControllable());
        Assert.assertTrue(teBuilder.getCommoditySoldList(0).getUsed() == 0);
    }

    @Test
    public void testReplaceSoldZeroCapacity() {
        final TopologyEntityDTO.Builder teBuilder = entityBuilder()
            .addCommoditySoldList(zeroCapacityCommoditySold());
        entityValidator
            .processIllegalCommodityValues(teBuilder, Optional.empty());
        Assert.assertFalse(teBuilder.getCommoditySoldList(0).hasCapacity());
        Assert.assertFalse(teBuilder.getAnalysisSettings().getControllable());
    }

    @Test
    public void testNoReplaceSoldUnsetCapacity() {
        final TopologyEntityDTO.Builder teBuilder =
            entityBuilder().addCommoditySoldList(noCapacityCommoditySold());
        entityValidator
            .processIllegalCommodityValues(teBuilder, Optional.empty());
        Assert.assertFalse(teBuilder.getCommoditySoldList(0).hasCapacity());
        Assert.assertFalse(teBuilder.getAnalysisSettings().getControllable());
    }

    @Test
    public void testReplaceBoughtUsed() {
        final TopologyEntityDTO.Builder teBuilder = entityBuilder()
            .addCommoditiesBoughtFromProviders(boughtFromProvider(badCommodityBought()));
        entityValidator
            .processIllegalCommodityValues(teBuilder, Optional.empty());
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
            .processIllegalCommodityValues(teBuilder, Optional.empty());
        Assert.assertTrue(
            teBuilder.getCommoditiesBoughtFromProviders(0).getCommodityBought(0).getUsed() == 0);
        Assert.assertTrue(
            teBuilder.getCommoditiesBoughtFromProviders(0).getCommodityBought(0).getPeak() == 0);
    }

    @Test
    public void testEmpty() throws Exception {
        entityValidator.validateTopologyEntities(Stream.empty(), false);
        //no exception thrown
    }

    @Test
    public void testEntitiesWithNoCommodities() throws EntitiesValidationException {
        entityValidator.validateTopologyEntities(
            Stream.of(TopologyEntity.newBuilder(entityBuilder()).build()), false);
        //no exception thrown
    }

    @Test
    public void testEntitiesWithGoodCommodities() throws EntitiesValidationException {
        entityValidator.validateTopologyEntities(Stream.of(TopologyEntity.newBuilder(entityBuilder()
            .addCommoditiesBoughtFromProviders(boughtFromProvider(goodCommodityBought()))
            .addCommoditySoldList(goodCommoditySold())).build()), false);
        //no exception thrown
    }

    @Test
    public void testEntitiesWithBadCommodities() throws EntitiesValidationException {
        final TopologyEntity te = TopologyEntity.newBuilder(entityBuilder()
            .addCommoditiesBoughtFromProviders(boughtFromProvider(badCommodityBought()))
            .addCommoditySoldList(badCommoditySold())).build();
        entityValidator.validateTopologyEntities(Stream.of(te), false);
        final Builder result = te.getTopologyEntityDtoBuilder();
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
        final TopologyEntity te = TopologyEntity.newBuilder(entityBuilder()
            .addCommoditySoldList(badCommoditySold())).build();
        ev.validateTopologyEntities(Stream.of(te), false);
        final Builder result = te.getTopologyEntityDtoBuilder();
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
        final TopologyEntity te1 = TopologyEntity.newBuilder(entityBuilder()
            .addCommoditySoldList(goodCommoditySold())).build();
        ev.validateTopologyEntities(Stream.of(te1), false);
        final TopologyEntity te2 = TopologyEntity.newBuilder(entityBuilder()
                        .addCommoditySoldList(badCommoditySold())).build();
        ev.validateTopologyEntities(Stream.of(te2), false);
        final Builder result = te2.getTopologyEntityDtoBuilder();
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
        final TopologyEntity te1 = TopologyEntity.newBuilder(entityBuilder()
            .addCommoditySoldList(goodCommoditySold())).build();
        ev.validateTopologyEntities(Stream.of(te1), false);
        final TopologyEntity te2 = TopologyEntity.newBuilder(entityBuilder()
                        .addCommoditySoldList(badCommoditySold()))
                        .setClonedFromEntity(te1.getTopologyEntityDtoBuilder())
                        .build();
        ev.validateTopologyEntities(Stream.of(te2), true);
        final Builder result = te2.getTopologyEntityDtoBuilder();
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
        final TopologyEntity te1 = TopologyEntity.newBuilder(entityBuilder()
            .addCommoditySoldList(goodCommoditySold())).build();
        ev.validateTopologyEntities(Stream.of(te1), true);
        final TopologyEntity te2 = TopologyEntity.newBuilder(entityBuilder()
                        .addCommoditySoldList(badCommoditySold())).build();
        ev.validateTopologyEntities(Stream.of(te2), false);
        final Builder result = te2.getTopologyEntityDtoBuilder();
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

}
