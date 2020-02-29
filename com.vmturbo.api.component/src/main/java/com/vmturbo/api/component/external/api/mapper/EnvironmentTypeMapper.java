package com.vmturbo.api.component.external.api.mapper;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;

/**
 * Conversions between different classes that represent environment types.
 */
public class EnvironmentTypeMapper {
    /**
     * Convert an API environment type value to an internal XL value.
     *
     * @param input an API environment type value.
     * @return the corresponding value in the type used internally by XL.
     */
    @Nullable
    public static EnvironmentType fromApiToXL(
            @Nullable com.vmturbo.api.enums.EnvironmentType input) {
        if (input == null) {
            return null;
        }
        switch (input) {
            case CLOUD:
                return EnvironmentType.CLOUD;
            case ONPREM:
                return EnvironmentType.ON_PREM;
            case HYBRID:
                return EnvironmentType.HYBRID;
            default:
                return EnvironmentType.UNKNOWN_ENV;
        }
    }

    /**
     * Convert an internal XL value to an API value.
     *
     * @param input an environment type value used internally in XL.
     * @return the corresponding API value.
     */
    @Nonnull
    public static com.vmturbo.api.enums.EnvironmentType fromXLToApi(
            @Nonnull EnvironmentType input) {
        switch (input) {
            case CLOUD:
                return com.vmturbo.api.enums.EnvironmentType.CLOUD;
            case ON_PREM:
                return com.vmturbo.api.enums.EnvironmentType.ONPREM;
            case HYBRID:
                return com.vmturbo.api.enums.EnvironmentType.HYBRID;
            default:
                return com.vmturbo.api.enums.EnvironmentType.UNKNOWN;
        }
    }
}
