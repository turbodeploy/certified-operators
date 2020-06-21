package com.vmturbo.common.api.mappers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;

/**
 * Utility for mapping been ENUMs com.vmturbo.api.enums.EnvironmentType and com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType.
 */
public class EnvironmentTypeMapper {

    /**
     * Mappings between {@link EnvironmentType} and {@link com.vmturbo.api.enums.EnvironmentType}.
     */
    private static final BiMap<EnvironmentType, com.vmturbo.api.enums.EnvironmentType> ENVIRONMENT_TYPE_MAPPINGS =
            new ImmutableBiMap.Builder()
                    .put(EnvironmentType.CLOUD, com.vmturbo.api.enums.EnvironmentType.CLOUD)
                    .put(EnvironmentType.ON_PREM, com.vmturbo.api.enums.EnvironmentType.ONPREM)
                    .put(EnvironmentType.HYBRID, com.vmturbo.api.enums.EnvironmentType.HYBRID)
                    .put(EnvironmentType.UNKNOWN_ENV, com.vmturbo.api.enums.EnvironmentType.UNKNOWN)
                    .build();


    /**
     * Private constructor, never initialized, pattern for a utility class.
     */
    private EnvironmentTypeMapper(){}

    /**
     * Get the {@link com.vmturbo.api.enums.EnvironmentType} associated with a {@link com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType}.
     *
     * @param environmentType The {@link EnvironmentType}.
     * @return The associated {@link com.vmturbo.api.enums.EnvironmentType},
     *         or {@link com.vmturbo.api.enums.EnvironmentType#UNKNOWN}.
     */
    @Nonnull
    public static com.vmturbo.api.enums.EnvironmentType fromXLToApi(@Nullable final EnvironmentType environmentType) {
        return ENVIRONMENT_TYPE_MAPPINGS.getOrDefault(environmentType, com.vmturbo.api.enums.EnvironmentType.UNKNOWN);
    }

    /**
     * Get the {@link EnvironmentType} associated with a {@link com.vmturbo.api.enums.EnvironmentType}.
     *
     * @param environmentType The {@link EnvironmentType}.
     * @return The associated {@link EnvironmentType},
     *         or {@link EnvironmentType#UNKNOWN_ENV}.
     */
    @Nonnull
    public static EnvironmentType fromApiToXL(@Nullable final com.vmturbo.api.enums.EnvironmentType environmentType) {
        return ENVIRONMENT_TYPE_MAPPINGS.inverse().getOrDefault(environmentType, EnvironmentType.UNKNOWN_ENV);

    }
}
