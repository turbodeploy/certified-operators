package com.vmturbo.common.api.mappers;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;

public class EnvironmentTypeMapperTest {

    /**
     * Testing converting EnvironmentType XL Enum to API Enum.
     */
    @Test
    public void fromXLToApi() {
        assertEquals(EnvironmentTypeMapper.fromXLToApi(EnvironmentTypeEnum.EnvironmentType.CLOUD), EnvironmentType.CLOUD);
        assertEquals(EnvironmentTypeMapper.fromXLToApi(EnvironmentTypeEnum.EnvironmentType.ON_PREM), EnvironmentType.ONPREM);
        assertEquals(EnvironmentTypeMapper.fromXLToApi(EnvironmentTypeEnum.EnvironmentType.HYBRID), EnvironmentType.HYBRID);
        assertEquals(EnvironmentTypeMapper.fromXLToApi(EnvironmentTypeEnum.EnvironmentType.UNKNOWN_ENV), EnvironmentType.UNKNOWN);
        assertEquals(EnvironmentTypeMapper.fromXLToApi(null), EnvironmentType.UNKNOWN);
    }

    /**
     * Testing converting EnvironmentType API Enum to XL Enum.
     */
    @Test
    public void fromApiToXl() {
        assertEquals(EnvironmentTypeMapper.fromApiToXL(EnvironmentType.CLOUD), EnvironmentTypeEnum.EnvironmentType.CLOUD);
        assertEquals(EnvironmentTypeMapper.fromApiToXL(EnvironmentType.ONPREM), EnvironmentTypeEnum.EnvironmentType.ON_PREM);
        assertEquals(EnvironmentTypeMapper.fromApiToXL(EnvironmentType.HYBRID), EnvironmentTypeEnum.EnvironmentType.HYBRID);
        assertEquals(EnvironmentTypeMapper.fromApiToXL(EnvironmentType.UNKNOWN), EnvironmentTypeEnum.EnvironmentType.UNKNOWN_ENV);
        assertEquals(EnvironmentTypeMapper.fromApiToXL(null), null);
    }
}
