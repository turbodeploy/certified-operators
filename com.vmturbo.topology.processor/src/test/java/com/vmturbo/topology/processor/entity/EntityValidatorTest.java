package com.vmturbo.topology.processor.entity;

import java.util.Optional;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
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
    public void testEmpty() {
        final Optional<EntityValidationFailure> error = entityValidator.validateEntityDTO(entityId,
                entityBuilder()
                    .setId("boo")
                    .build());
        Assert.assertFalse(error.isPresent());
    }

    @Test
    public void testGoodCommodityBought() {
        final Optional<EntityValidationFailure> error = entityValidator.validateEntityDTO(entityId,
                entityBuilder()
                    .addCommoditiesBought(noErrorCommBought())
                    .build());
        Assert.assertFalse(error.isPresent());
    }

    @Test
    public void testBadCommodityBought() {
        final Optional<EntityValidationFailure> error = entityValidator.validateEntityDTO(entityId,
                entityBuilder()
                    .addCommoditiesBought(errorCommBought())
                    .build());
        Assert.assertTrue(error.isPresent());
    }

    @Test
    public void testGoodCommoditySold() {
        final Optional<EntityValidationFailure> error = entityValidator.validateEntityDTO(entityId,
                entityBuilder()
                    .addCommoditiesSold(noErrorCommodity())
                    .build());
        Assert.assertFalse(error.isPresent());
    }

    @Test
    public void testBadCommoditySold() {
        final Optional<EntityValidationFailure> error = entityValidator.validateEntityDTO(entityId,
                entityBuilder()
                    .addCommoditiesSold(errorCommodity())
                    .build());
        Assert.assertTrue(error.isPresent());
    }

    @Test
    public void testBadCommoditySoldAndBought() {
        final Optional<EntityValidationFailure> error = entityValidator.validateEntityDTO(entityId,
                entityBuilder()
                    .addCommoditiesSold(errorCommodity())
                    .addCommoditiesBought(errorCommBought())
                    .build());
        Assert.assertTrue(error.isPresent());
    }

    @Test
    public void testReplaceSoldCapacity() {
        final CommodityDTO commodity = CommodityDTO.newBuilder()
            .setCommodityType(CommodityType.CPU)
            .setCapacity(-1)
            .build();
        final EntityDTO ownerEntity = entityBuilder()
                .addCommoditiesSold(commodity)
                .build();
        final CommodityDTO newCommodity =
                entityValidator.replaceIllegalCommodityValues(ownerEntity, commodity, true);
        Assert.assertTrue(newCommodity.getCapacity() > 0);
    }

    @Test
    public void testReplaceSoldZeroCapacity() {
        final CommodityDTO commodity = CommodityDTO.newBuilder()
                .setCommodityType(CommodityType.CPU)
                .setCapacity(0.0)
                .build();
        final EntityDTO ownerEntity = entityBuilder()
                .addCommoditiesSold(commodity)
                .build();
        final CommodityDTO newCommodity =
                entityValidator.replaceIllegalCommodityValues(ownerEntity, commodity, true);
        Assert.assertTrue(newCommodity.getCapacity() > 0);
    }

    @Test
    public void testReplaceSoldUnsetCapacity() {
        final CommodityDTO commodity = CommodityDTO.newBuilder()
                .setCommodityType(CommodityType.CPU)
                .build();
        final EntityDTO ownerEntity = entityBuilder()
                .addCommoditiesSold(commodity)
                .build();
        final CommodityDTO newCommodity =
                entityValidator.replaceIllegalCommodityValues(ownerEntity, commodity, true);
        Assert.assertTrue(newCommodity.getCapacity() > 0);
    }

    @Test
    public void testNotReplaceBoughtCapacity() {
        final CommodityDTO commodity = CommodityDTO.newBuilder()
                .setCommodityType(CommodityType.CPU)
                .setCapacity(-1)
                .build();
        final EntityDTO ownerEntity = entityBuilder()
                .addCommoditiesSold(commodity)
                .build();
        final CommodityDTO newCommodity =
                entityValidator.replaceIllegalCommodityValues(ownerEntity, commodity, false);
        Assert.assertEquals(commodity.getCapacity(), newCommodity.getCapacity(), 0.0);
    }

    @Test
    public void testReplaceSoldUsed() {
        final CommodityDTO commodity = CommodityDTO.newBuilder()
                .setCommodityType(CommodityType.CPU)
                .setUsed(-1)
                .build();
        final EntityDTO ownerEntity = entityBuilder()
                .addCommoditiesSold(commodity)
                .build();
        final CommodityDTO newCommodity =
                entityValidator.replaceIllegalCommodityValues(ownerEntity, commodity, true);
        Assert.assertTrue(newCommodity.getUsed() >= 0);
    }

    @Test
    public void testReplaceBoughtUsed() {
        final CommodityDTO commodity = CommodityDTO.newBuilder()
                .setCommodityType(CommodityType.CPU)
                .setUsed(-1)
                .build();
        final EntityDTO ownerEntity = entityBuilder()
                .addCommoditiesBought(CommodityBought.newBuilder()
                    .setProviderId("foo")
                    .addBought(commodity))
                .build();
        final CommodityDTO newCommodity =
                entityValidator.replaceIllegalCommodityValues(ownerEntity, commodity, false);
        Assert.assertTrue(newCommodity.getUsed() >= 0);
    }

    private EntityDTO.Builder entityBuilder() {
        return EntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .setId("boo");
    }

    private CommodityBought errorCommBought() {
        return CommodityBought.newBuilder()
                .setProviderId("provider")
                .addBought(errorCommodity())
                .build();
    }

    private CommodityBought noErrorCommBought() {
        return CommodityBought.newBuilder()
                .setProviderId("provider")
                .addBought(noErrorCommodity())
                .build();
    }

    /**
     * Returns the worst possible commodity.
     * If necessary, can augment this method to return a commodity with
     * the desired number of bad fields.
     *
     * @return The bad commodity.
     */
    private CommodityDTO errorCommodity() {
        // Negative numbers for capacity, used, and reservation are illegal.
        return CommodityDTO.newBuilder()
                .setCommodityType(CommodityType.CPU)
                .setCapacity(-1)
                .setUsed(-1)
                .setReservation(-1)
                .build();
    }

    private CommodityDTO noErrorCommodity() {
        return CommodityDTO.newBuilder()
                .setCommodityType(CommodityType.CPU)
                .setCapacity(1)
                .setUsed(0)
                .setReservation(0)
                .build();

    }
}
