package com.vmturbo.stitching.utilities;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ActionOnProviderEligibility;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;

/**
 * Tests for methods CopyActionEligibility.
 */
public class CopyActionEligibilityTest {

    /**
     * Test transfer of action eligibility from EntityDTO to CommoditiesBought.
     */
    @Test
    public void transferToCommoditiesBought() {
        CommonDTO.EntityDTO.CommodityBought commodityBought
                = CommodityBought.newBuilder()
                .setProviderId("provider1")
                .setActionEligibility(ActionOnProviderEligibility.newBuilder()
                        .setMovable(false)
                        .setStartable(true)
                        .build())
                .build();

        List<CommodityDTO.Builder> boughtList = new ArrayList<>();
        CommodityDTO.Builder commDto = CommodityDTO.newBuilder();
        boughtList.add(commDto);
        CommoditiesBought commoditiesBought = new CommoditiesBought(boughtList);

        CopyActionEligibility.transferActionEligibilitySettingsFromEntityDTO(commodityBought,
                commoditiesBought);

        assertTrue(commoditiesBought.getMovable().isPresent());
        assertFalse(commoditiesBought.getMovable().get());

        assertTrue(commoditiesBought.getStartable().isPresent());
        assertTrue(commoditiesBought.getStartable().get());

        assertFalse(commoditiesBought.getScalable().isPresent());
    }

    /**
     * Test transfer of action eligibility from EntityDTO to CommoditiesBought,
     * when the settings are not provided.
     */
    @Test
    public void transferMissingActionEligibilityToCommoditiesBought() {
        CommonDTO.EntityDTO.CommodityBought commodityBought
                = CommodityBought.newBuilder()
                .setProviderId("provider1")
                .build();

        List<CommodityDTO.Builder> boughtList = new ArrayList<>();
        CommodityDTO.Builder commDto = CommodityDTO.newBuilder();
        boughtList.add(commDto);
        CommoditiesBought commoditiesBought = new CommoditiesBought(boughtList);

        CopyActionEligibility.transferActionEligibilitySettingsFromEntityDTO(commodityBought,
                commoditiesBought);

        assertFalse(commoditiesBought.getMovable().isPresent());
        assertFalse(commoditiesBought.getStartable().isPresent());
        assertFalse(commoditiesBought.getScalable().isPresent());
    }

    /**
     * Test transfer of action eligibility from EntityDTO to CommoditiesBought,
     * when the action eligibility is provided but contains no settings for any actions.
     */
    @Test
    public void transferEmptyActionEligibilityToCommoditiesBought() {
        CommonDTO.EntityDTO.CommodityBought commodityBought
                = CommodityBought.newBuilder()
                .setProviderId("provider1")
                .setActionEligibility(ActionOnProviderEligibility.newBuilder()
                        .build())
                .build();

        List<CommodityDTO.Builder> boughtList = new ArrayList<>();
        CommodityDTO.Builder commDto = CommodityDTO.newBuilder();
        boughtList.add(commDto);
        CommoditiesBought commoditiesBought = new CommoditiesBought(boughtList);

        CopyActionEligibility.transferActionEligibilitySettingsFromEntityDTO(commodityBought,
                commoditiesBought);

        assertFalse(commoditiesBought.getMovable().isPresent());
        assertFalse(commoditiesBought.getStartable().isPresent());
        assertFalse(commoditiesBought.getScalable().isPresent());
    }

    /**
     * Test transfer of action eligibility from EntityDTO to TopologyDTO.
     */
    @Test
    public void transferToTopologyDTO() {
        CommonDTO.EntityDTO.CommodityBought commodityBought
                = CommodityBought.newBuilder()
                .setProviderId("provider1")
                .setActionEligibility(ActionOnProviderEligibility.newBuilder()
                        .setMovable(false)
                        .setStartable(true)
                        .build())
                .build();

        CommoditiesBoughtFromProvider.Builder cbBuilder = CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(100L);

        CopyActionEligibility.transferActionEligibilitySettingsFromEntityDTO(commodityBought,
                cbBuilder);

        assertTrue(cbBuilder.hasMovable());
        assertTrue(cbBuilder.hasStartable());
        assertFalse(cbBuilder.hasScalable());
    }

    /**
     * Test transfer of action eligibility from EntityDTO to TopologyDTO,
     * when the settings are not provided.
     */
    @Test
    public void transferMissingActionEligibilityToTopologyDTO() {
        CommonDTO.EntityDTO.CommodityBought commodityBought
                = CommodityBought.newBuilder()
                .setProviderId("provider1")
                .build();

        CommoditiesBoughtFromProvider.Builder cbBuilder = CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(100L);

        CopyActionEligibility.transferActionEligibilitySettingsFromEntityDTO(commodityBought, cbBuilder);

        assertFalse(cbBuilder.hasMovable());
        assertFalse(cbBuilder.hasStartable());
        assertFalse(cbBuilder.hasScalable());
    }

    /**
     * Test transfer of action eligibility from EntityDTO to TopologyDTO,
     * when the action eligibility is provided but contains no settings for any actions.
     */
    @Test
    public void transferEmptyActionEligibilityToTopologyDTO() {
        CommonDTO.EntityDTO.CommodityBought commodityBought
                = CommodityBought.newBuilder()
                .setProviderId("provider1")
                .setActionEligibility(ActionOnProviderEligibility.newBuilder()
                        .build())
                .build();

        CommoditiesBoughtFromProvider.Builder cbBuilder = CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(100L);

        CopyActionEligibility.transferActionEligibilitySettingsFromEntityDTO(commodityBought, cbBuilder);

        assertFalse(cbBuilder.hasMovable());
        assertFalse(cbBuilder.hasStartable());
        assertFalse(cbBuilder.hasScalable());
    }
}
