package com.vmturbo.common.protobuf.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;

/**
 * Tests for utility methods of {@link GuestLoadFilters}.
 */
public class GuestLoadFiltersTest {
    /**
     * Guest Load application with {@code APPLICATION} entity type.
     */
    public final TopologyEntityDTO guestLoadApp = TopologyEntityDTO.newBuilder()
            .setOid(1L)
            .setDisplayName("GuestLoad[vm1]")
            .setEntityType(ApiEntityType.APPLICATION.typeNumber())
            .setEntityState(EntityState.POWERED_ON)
            .putEntityPropertyMap(GuestLoadFilters.APPLICATION_TYPE_PATH, SupplyChainConstants.GUEST_LOAD)
            .build();
    /**
     * Guest Load application with {@code APPLICATION_COMPONENT} entity type.
     */
    public final TopologyEntityDTO guestLoadAppComponent = TopologyEntityDTO.newBuilder()
            .setOid(2L)
            .setDisplayName("GuestLoad[vm2]")
            .setEntityType(ApiEntityType.APPLICATION_COMPONENT.typeNumber())
            .setEntityState(EntityState.POWERED_ON)
            .putEntityPropertyMap(GuestLoadFilters.APPLICATION_TYPE_PATH, SupplyChainConstants.GUEST_LOAD)
            .build();
    /**
     * Usual (not guest load) application.
     */
    public final TopologyEntityDTO application = TopologyEntityDTO.newBuilder()
            .setOid(3L)
            .setDisplayName("Application")
            .setEntityType(ApiEntityType.APPLICATION_COMPONENT.typeNumber())
            .setEntityState(EntityState.POWERED_ON)
            .build();
    /**
     * Virtual Machine (not application type).
     */
    public final TopologyEntityDTO vm = TopologyEntityDTO.newBuilder()
            .setOid(4L)
            .setDisplayName("VM")
            .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
            .setEntityState(EntityState.POWERED_ON)
            .build();

    /**
     * Test for {@link GuestLoadFilters#isGuestLoad(TopologyEntityDTO)} method.
     */
    @Test
    public void isGuestLoadTopologyEntityDTO() {
        assertTrue(GuestLoadFilters.isGuestLoad(guestLoadApp));
        assertTrue(GuestLoadFilters.isGuestLoad(guestLoadAppComponent));

        assertFalse(GuestLoadFilters.isGuestLoad(application));
        assertFalse(GuestLoadFilters.isGuestLoad(vm));
    }

    /**
     * Test for {@link GuestLoadFilters#isNotGuestLoad(TopologyEntityDTO)} method.
     */
    @Test
    public void isNotGuestLoadRepoGraphEntity() {
        assertFalse(GuestLoadFilters.isNotGuestLoad(guestLoadApp));
        assertFalse(GuestLoadFilters.isNotGuestLoad(guestLoadAppComponent));

        assertTrue(GuestLoadFilters.isNotGuestLoad(application));
        assertTrue(GuestLoadFilters.isNotGuestLoad(vm));
    }
}
