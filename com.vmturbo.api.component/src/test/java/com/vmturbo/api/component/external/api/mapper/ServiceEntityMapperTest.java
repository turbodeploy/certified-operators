package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.*;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;

public class ServiceEntityMapperTest {

    @Test
    public void testState() {
        assertEquals("ACTIVE", ServiceEntityMapper.toState(EntityState.POWERED_ON_VALUE));
        assertEquals("IDLE", ServiceEntityMapper.toState(EntityState.POWERED_OFF_VALUE));
        assertEquals("MAINTENANCE", ServiceEntityMapper.toState(EntityState.MAINTENANCE_VALUE));
        assertEquals("FAILOVER", ServiceEntityMapper.toState(EntityState.FAILOVER_VALUE));
        assertEquals("SUSPENDED", ServiceEntityMapper.toState(EntityState.SUSPENDED_VALUE));
        // Any other value maps to UNKNOWN
        assertEquals("UNKNOWN", ServiceEntityMapper.toState(EntityState.UNKNOWN_VALUE));
        assertEquals("UNKNOWN", ServiceEntityMapper.toState(EntityState.UNKNOWN_VALUE + 1));
        assertEquals("UNKNOWN", ServiceEntityMapper.toState(-1));
    }

}
