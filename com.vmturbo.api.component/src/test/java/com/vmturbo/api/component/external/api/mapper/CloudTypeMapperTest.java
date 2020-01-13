package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Optional;

import org.junit.Test;

import com.vmturbo.api.enums.CloudType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Unit tests for {@link CloudTypeMapper} class.
 */
public class CloudTypeMapperTest {

    /**
     * Test {@link CloudTypeMapper#fromTargetType(String)} method when it returns not null value.
     */
    @Test
    public void testFromTargetTypeNotNull() {
        final CloudTypeMapper cloudTypeMapper = new CloudTypeMapper();

        assertEquals(Optional.of(CloudType.AWS), cloudTypeMapper.fromTargetType(
            SDKProbeType.AWS.getProbeType()));
        assertEquals(Optional.of(CloudType.AZURE), cloudTypeMapper.fromTargetType(
            SDKProbeType.AZURE_EA.getProbeType()));
    }


    /**
     * Test {@link CloudTypeMapper#fromTargetType(String)} method when it returns null value.
     */
    @Test
    public void testFromTargetTypeNull() {
        final CloudTypeMapper cloudTypeMapper = new CloudTypeMapper();

        assertEquals(Optional.empty(), cloudTypeMapper.fromTargetType("Unknown target type"));
    }
}
