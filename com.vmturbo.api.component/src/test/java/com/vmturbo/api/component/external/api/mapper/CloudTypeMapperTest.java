package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Optional;

import org.junit.Test;

import com.vmturbo.api.enums.CloudType;
import com.vmturbo.common.protobuf.common.CloudTypeEnum;
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

    /**
     * Tests that {@link CloudTypeMapper#fromApiToXlProtoEnum(CloudType)} works.
     */
    @Test
    public void testFromApiToXl() {
        assertEquals(CloudTypeEnum.CloudType.UNKNOWN_CLOUD,
                CloudTypeMapper.fromApiToXlProtoEnum(CloudType.UNKNOWN));
        assertEquals(CloudTypeEnum.CloudType.AWS,
                CloudTypeMapper.fromApiToXlProtoEnum(CloudType.AWS));
        assertEquals(CloudTypeEnum.CloudType.AZURE,
                CloudTypeMapper.fromApiToXlProtoEnum(CloudType.AZURE));
        assertEquals(CloudTypeEnum.CloudType.GCP,
                CloudTypeMapper.fromApiToXlProtoEnum(CloudType.GCP));
        assertEquals(CloudTypeEnum.CloudType.HYBRID_CLOUD,
                CloudTypeMapper.fromApiToXlProtoEnum(CloudType.HYBRID));
        assertEquals(CloudTypeEnum.CloudType.UNKNOWN_CLOUD,
                CloudTypeMapper.fromApiToXlProtoEnum(null));
    }

    /**
     * Tests that {@link CloudTypeMapper#fromXlProtoEnumToApi(CloudTypeEnum.CloudType)} works.
     */
    @Test
    public void testFromXlToApi() {
        assertEquals(CloudType.UNKNOWN,
                CloudTypeMapper.fromXlProtoEnumToApi(CloudTypeEnum.CloudType.UNKNOWN_CLOUD));
        assertEquals(CloudType.AWS,
                CloudTypeMapper.fromXlProtoEnumToApi(CloudTypeEnum.CloudType.AWS));
        assertEquals(CloudType.AZURE,
                CloudTypeMapper.fromXlProtoEnumToApi(CloudTypeEnum.CloudType.AZURE));
        assertEquals(CloudType.GCP,
                CloudTypeMapper.fromXlProtoEnumToApi(CloudTypeEnum.CloudType.GCP));
        assertEquals(CloudType.HYBRID,
                CloudTypeMapper.fromXlProtoEnumToApi(CloudTypeEnum.CloudType.HYBRID_CLOUD));
        assertEquals(CloudType.UNKNOWN, CloudTypeMapper.fromXlProtoEnumToApi(null));
    }
}
