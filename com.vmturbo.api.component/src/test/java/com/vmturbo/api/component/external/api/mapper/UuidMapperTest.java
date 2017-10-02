package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;

public class UuidMapperTest {

    @Test
    public void testRealtimeMarketId() {
        UuidMapper mapper = new UuidMapper(7L);
        ApiId id = mapper.fromUuid(UuidMapper.UI_REAL_TIME_MARKET_STR);
        assertTrue(id.isRealtimeMarket());
        assertEquals(7L, id.oid());
        assertEquals(UuidMapper.UI_REAL_TIME_MARKET_STR, id.uuid());
    }
}
