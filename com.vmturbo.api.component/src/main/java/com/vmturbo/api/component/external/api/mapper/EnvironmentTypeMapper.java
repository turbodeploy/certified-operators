package com.vmturbo.api.component.external.api.mapper;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.UIEnvironmentType;

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
    @Nonnull
    public static Optional<EnvironmentType> fromApiToXL(
            @Nonnull com.vmturbo.api.enums.EnvironmentType input) {
        return UIEnvironmentType.fromString(input.name()).toEnvType();
    }

    /**
     * Convert an internal XL value to an API value.
     *
     * @param input an environment type value used internally in XL.
     * @return the corresponding API value.
     */
    @Nonnull
    public static Optional<com.vmturbo.api.enums.EnvironmentType> fromXLToApi(
            @Nonnull EnvironmentType input) {
        switch (input) {
            case CLOUD:
                return Optional.of(com.vmturbo.api.enums.EnvironmentType.CLOUD);
            case ON_PREM:
                return Optional.of(com.vmturbo.api.enums.EnvironmentType.ONPREM);
            case HYBRID:
                return Optional.of(com.vmturbo.api.enums.EnvironmentType.HYBRID);
            case UNKNOWN_ENV:
                return Optional.of(com.vmturbo.api.enums.EnvironmentType.UNKNOWN);
            default:
                return Optional.empty();
        }
    }

    /**
     * Check if the given environment type matches the user requested environment type.
     *
     * @param reqEnvType user requested environment type
     * @param envToCheck the environment type to check
     * @return true if the environment type matches requested environment type
     */
    public static boolean matches(@Nullable com.vmturbo.api.enums.EnvironmentType reqEnvType,
            @Nullable com.vmturbo.api.enums.EnvironmentType envToCheck) {
        if (reqEnvType == null || reqEnvType == com.vmturbo.api.enums.EnvironmentType.HYBRID) {
            return true;
        }
        return reqEnvType == envToCheck;
    }
}
