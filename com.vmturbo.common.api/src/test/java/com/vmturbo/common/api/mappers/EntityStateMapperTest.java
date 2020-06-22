package com.vmturbo.common.api.mappers;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;

public class EntityStateMapperTest {

    /**
     * Testing converting EntityState XL Enum to API Enum.
     */
    @Test
    public void fromXLToApi() {
        assertEquals(EntityStateMapper.fromXLToApi(EntityState.POWERED_ON), com.vmturbo.api.enums.EntityState.ACTIVE);
        assertEquals(EntityStateMapper.fromXLToApi(EntityState.POWERED_OFF), com.vmturbo.api.enums.EntityState.IDLE);
        assertEquals(EntityStateMapper.fromXLToApi(EntityState.FAILOVER), com.vmturbo.api.enums.EntityState.FAILOVER);
        assertEquals(EntityStateMapper.fromXLToApi(EntityState.MAINTENANCE), com.vmturbo.api.enums.EntityState.MAINTENANCE);
        assertEquals(EntityStateMapper.fromXLToApi(EntityState.SUSPENDED), com.vmturbo.api.enums.EntityState.SUSPEND);
        assertEquals(EntityStateMapper.fromXLToApi(EntityState.UNKNOWN), com.vmturbo.api.enums.EntityState.UNKNOWN);
    }

    /**
     * Testing converting EntityState API Enum to XL Enum.
     */
    @Test
    public void fromApiToXL() {
        assertEquals(EntityStateMapper.fromApiToXL(com.vmturbo.api.enums.EntityState.ACTIVE), EntityState.POWERED_ON);
        assertEquals(EntityStateMapper.fromApiToXL(com.vmturbo.api.enums.EntityState.IDLE), EntityState.POWERED_OFF);
        assertEquals(EntityStateMapper.fromApiToXL(com.vmturbo.api.enums.EntityState.FAILOVER), EntityState.FAILOVER);
        assertEquals(EntityStateMapper.fromApiToXL(com.vmturbo.api.enums.EntityState.MAINTENANCE), EntityState.MAINTENANCE);
        assertEquals(EntityStateMapper.fromApiToXL(com.vmturbo.api.enums.EntityState.SUSPEND), EntityState.SUSPENDED);
        assertEquals(EntityStateMapper.fromApiToXL(com.vmturbo.api.enums.EntityState.UNKNOWN), EntityState.UNKNOWN);
    }
}
