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

    private final EntityValidator entityValidator = new EntityValidator();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final long entityId = 7;

    @Test
    public void testNoCommodities() throws Exception {
        final Optional<EntityValidationFailure> error =
            entityValidator.validateSingleEntity(entityBuilder());
        Assert.assertFalse(error.isPresent());
    }

    @Test
    public void testGoodCommodityBought() {
        final Optional<EntityValidationFailure> error =
            entityValidator.validateSingleEntity(
                entityBuilder().addCommoditiesBoughtFromProviders(
                    boughtFromProvider(goodCommodityBought())));
        Assert.assertFalse(error.isPresent());
    }

    @Test
    public void testBadCommodityBought() {
        final Optional<EntityValidationFailure> error =
            entityValidator.validateSingleEntity(
                entityBuilder().addCommoditiesBoughtFromProviders(
                    boughtFromProvider(badCommodityBought())));
        Assert.assertTrue(error.isPresent());
    }

    @Test
    public void testGoodCommoditySold() {
        final Optional<EntityValidationFailure> error = entityValidator.validateSingleEntity(
                entityBuilder().addCommoditySoldList(goodCommoditySold()));
        Assert.assertFalse(error.isPresent());
    }

    @Test
    public void testBadCommoditySold() {
        final Optional<EntityValidationFailure> error = entityValidator.validateSingleEntity(
            entityBuilder().addCommoditySoldList(badCommoditySold()));
        Assert.assertTrue(error.isPresent());
    }

    @Test
    public void testBadCommoditySoldAndBought() {
        final Optional<EntityValidationFailure> error = entityValidator.validateSingleEntity(
                entityBuilder()
                    .addCommoditySoldList(badCommoditySold())
                    .addCommoditiesBoughtFromProviders(boughtFromProvider(badCommodityBought())));
        Assert.assertTrue(error.isPresent());
    }

    @Test
    public void testReplaceSoldCapacity() {
        final TopologyEntityDTO.Builder teBuilder = entityBuilder()
            .addCommoditySoldList(badCommoditySold());
        entityValidator
            .replaceIllegalCommodityValues(teBuilder);
        Assert.assertTrue(teBuilder.getCommoditySoldList(0).getCapacity() > 0);
        Assert.assertTrue(teBuilder.getCommoditySoldList(0).getUsed() >= 0);
        Assert.assertTrue(teBuilder.getCommoditySoldList(0).getPeak() >= 0);
    }

    @Test
    public void testReplaceSoldZeroCapacity() {
        final TopologyEntityDTO.Builder teBuilder = entityBuilder()
            .addCommoditySoldList(zeroCapacityCommoditySold());
        entityValidator
            .replaceIllegalCommodityValues(teBuilder);
        Assert.assertTrue(teBuilder.getCommoditySoldList(0).getCapacity() > 0);
    }

    @Test
    public void testReplaceSoldUnsetCapacity() {
        final TopologyEntityDTO.Builder teBuilder =
            entityBuilder().addCommoditySoldList(noCapacityCommoditySold());
        entityValidator
            .replaceIllegalCommodityValues(teBuilder);
        Assert.assertTrue(teBuilder.getCommoditySoldList(0).getCapacity() > 0);
    }

    @Test
    public void testReplaceBoughtUsed() {
        final TopologyEntityDTO.Builder teBuilder = entityBuilder()
            .addCommoditiesBoughtFromProviders(boughtFromProvider(badCommodityBought()));
        entityValidator
            .replaceIllegalCommodityValues(teBuilder);
        Assert.assertTrue(
            teBuilder.getCommoditiesBoughtFromProviders(0).getCommodityBought(0).getUsed() >= 0);
        Assert.assertTrue(
            teBuilder.getCommoditiesBoughtFromProviders(0).getCommodityBought(0).getPeak() >= 0);
    }

    @Test
    public void testEmpty() throws Exception {
        entityValidator.validateTopologyEntities(Stream.empty());
        //no exception thrown
    }

    @Test
    public void testEntitiesWithNoCommodities() throws Exception {
        entityValidator.validateTopologyEntities(
            Stream.of(TopologyEntity.newBuilder(entityBuilder()).build()));
        //no exception thrown
    }

    @Test
    public void testEntitiesWithGoodCommodities() throws Exception {

        entityValidator.validateTopologyEntities(Stream.of(TopologyEntity.newBuilder(entityBuilder()
            .addCommoditiesBoughtFromProviders(boughtFromProvider(goodCommodityBought()))
            .addCommoditySoldList(goodCommoditySold())).build()));
        //no exception thrown
    }

    @Test
    public void testEntitiesWithBadCommodities() throws Exception {
        final TopologyEntity te = TopologyEntity.newBuilder(entityBuilder()
            .addCommoditiesBoughtFromProviders(boughtFromProvider(badCommodityBought()))
            .addCommoditySoldList(badCommoditySold())).build();
        entityValidator.validateTopologyEntities(Stream.of(te));
        final Builder result = te.getTopologyEntityDtoBuilder();
        Assert.assertEquals(0,
            result.getCommoditiesBoughtFromProviders(0).getCommodityBought(0).getPeak(), 0);
        Assert.assertEquals(0,
            result.getCommoditiesBoughtFromProviders(0).getCommodityBought(0).getUsed(), 0);
        Assert.assertEquals(0, result.getCommoditySoldList(0).getPeak(), 0);
        Assert.assertEquals(0, result.getCommoditySoldList(0).getUsed(), 0);
        Assert.assertTrue(result.getCommoditySoldList(0).getCapacity() > 0);
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
