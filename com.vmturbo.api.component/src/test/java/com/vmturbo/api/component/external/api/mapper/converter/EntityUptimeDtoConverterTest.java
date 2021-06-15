package com.vmturbo.api.component.external.api.mapper.converter;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.api.dto.entity.EntityUptimeApiDTO;
import com.vmturbo.common.protobuf.cost.EntityUptime.EntityUptimeDTO;

/**
 * Unit test for {@link EntityUptimeDtoConverter}.
 */
public class EntityUptimeDtoConverterTest {

    private static final Long TOTAL_DURATION_MS = 1L;
    private static final Long UPTIME_DURATION_MS = 2L;
    private static final Long CREATION_TIME_MS = 3L;
    private static final double UPTIME_PERCENTAGE = 99.5;

    /**
     * Test for {@link EntityUptimeDtoConverter#convert(EntityUptimeDTO)}.
     */
    @Test
    public void testConvert() {
        final EntityUptimeApiDTO dto = new EntityUptimeDtoConverter().convert(
                EntityUptimeDTO.newBuilder()
                        .setTotalDurationMs(TOTAL_DURATION_MS)
                        .setUptimeDurationMs(UPTIME_DURATION_MS)
                        .setCreationTimeMs(CREATION_TIME_MS)
                        .setUptimePercentage(UPTIME_PERCENTAGE)
                        .build());

        Assert.assertEquals(TOTAL_DURATION_MS, dto.getTotalDurationInMilliseconds());
        Assert.assertEquals(UPTIME_DURATION_MS, dto.getUptimeDurationInMilliseconds());
        Assert.assertEquals(CREATION_TIME_MS, dto.getCreationTimestamp());
        Assert.assertEquals(UPTIME_PERCENTAGE, dto.getUptimePercentage(), 0.0001);
    }
}
