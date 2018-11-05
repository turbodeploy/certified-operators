package com.vmturbo.components.common.mapping;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;

/**
 * An enum to represent the UI/API's environment type (com.vmturbo.api.enums.EnvironmentType) and
 * mappings between it and {@link EnvironmentType} in XL's protobuf definitions.
 *
 * We don't use the API enum directly because we need to support searches over its string value,
 * (e.g. by specifying the environment type in a SearchParameter in Search.proto) and because we
 * want to avoid relying heavily on API types inside XL (other than the API component).
 */
public enum UIEnvironmentType {
    CLOUD("CLOUD"),
    ON_PREM("ONPREM"),
    UNKNOWN("UNKNOWN");

    private static final Logger logger = LogManager.getLogger();

    /**
     * Mappings between {@link EnvironmentType} enum values in TopologyEntityDTO to strings
     * that the UI expects.
     */
    private static final BiMap<EnvironmentType, UIEnvironmentType> ENTITY_STATE_MAPPINGS =
            new ImmutableBiMap.Builder<EnvironmentType, UIEnvironmentType>()
                    .put(EnvironmentType.CLOUD, UIEnvironmentType.CLOUD)
                    .put(EnvironmentType.ON_PREM, UIEnvironmentType.ON_PREM)
                    .put(EnvironmentType.UNKNOWN_ENV, UIEnvironmentType.UNKNOWN)
                    .build();

    /**
     * This value should be synchronized with com.vmturbo.api.enums.EnvironmentType.
     * We don't actually import the api ENUM, because we have to use the string value
     * in places where the enum is unaccessible (e.g. in search protobufs).
     */
    private String apiEnumStringValue;

    UIEnvironmentType(final String apiEnumStringValue) {
        this.apiEnumStringValue = apiEnumStringValue;
    }

    /**
     * @return The string value of the com.vmturbo.api.enums.EnvironmentType this enum represents.
     */
    @Nonnull
    public String getApiEnumStringValue() {
        return apiEnumStringValue;
    }

    /**
     * @param inputEnvType The input string. This should be the name of an enum in
     *                     com.vmturbo.api.enums.EnvironmentType.
     * @return The associated {@link UIEnvironmentType}, or {@link UIEnvironmentType#UNKNOWN}.
     */
    @Nonnull
    public static UIEnvironmentType fromString(@Nullable final String inputEnvType) {
        if (inputEnvType == null) {
            logger.warn("Returning UNKOWN entity state for empty string.");
            return UIEnvironmentType.UNKNOWN;
        } else {
            for (UIEnvironmentType envType : UIEnvironmentType.values()) {
                if (inputEnvType.equalsIgnoreCase(envType.getApiEnumStringValue())) {
                    return envType;
                }
            }
            logger.warn("Unhandled input state {}. Returning UNKNOWN.", inputEnvType);
            return UIEnvironmentType.UNKNOWN;
        }
    }

    /**
     * @return Get the {@link EnvironmentType} associated with this {@link UIEnvironmentType}.
     */
    @Nonnull
    public EnvironmentType toEnvType() {
        return ENTITY_STATE_MAPPINGS.inverse().getOrDefault(this, EnvironmentType.UNKNOWN_ENV);
    }

    /**
     * @param envType An {@link EnvironmentType} protobuf.
     * @return The associated {@link UIEnvironmentType}, or {@link UIEnvironmentType#UNKNOWN}.
     */
    @Nonnull
    public static UIEnvironmentType fromEnvType(@Nonnull final EnvironmentType envType) {
        return ENTITY_STATE_MAPPINGS.getOrDefault(envType, UIEnvironmentType.UNKNOWN);
    }
}
