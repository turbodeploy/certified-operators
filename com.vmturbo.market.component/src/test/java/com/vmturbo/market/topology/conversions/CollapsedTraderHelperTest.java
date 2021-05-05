package com.vmturbo.market.topology.conversions;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for CollapsedTraderHelper class.
 */
public class CollapsedTraderHelperTest {

    /**
     * Test getNewProviderTypeAfterCollapsing.
     */
    @Test
    public void testGetNewProviderTypeAfterCollapsing() {
        // Compute shopping lists of VM need no collapsing.
        assertTrue(CollapsedTraderHelper.getNewProviderTypeAfterCollapsing(
                EntityType.VIRTUAL_MACHINE_VALUE, EntityType.PHYSICAL_MACHINE_VALUE) == null);
        // Volume shopping lists of VM need collapsing.
        assertTrue(CollapsedTraderHelper.getNewProviderTypeAfterCollapsing(
                EntityType.VIRTUAL_MACHINE_VALUE, EntityType.VIRTUAL_VOLUME_VALUE).contains(EntityType.STORAGE_TIER_VALUE));
        assertTrue(CollapsedTraderHelper.getNewProviderTypeAfterCollapsing(
                EntityType.VIRTUAL_MACHINE_VALUE, EntityType.VIRTUAL_VOLUME_VALUE).contains(EntityType.STORAGE_VALUE));
    }

    /**
     * Test getCollapsedEntityType.
     */
    @Test
    public void testGetCollapsedEntityType() {
        assertTrue(CollapsedTraderHelper.getCollapsedEntityType(EntityType.APPLICATION_VALUE) == null);
        assertTrue(CollapsedTraderHelper.getCollapsedEntityType(EntityType.VIRTUAL_MACHINE_VALUE).equals(EntityType.VIRTUAL_VOLUME_VALUE));
    }
}