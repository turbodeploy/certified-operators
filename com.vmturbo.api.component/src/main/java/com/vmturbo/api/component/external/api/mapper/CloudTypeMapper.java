package com.vmturbo.api.component.external.api.mapper;

import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.enums.CloudType;
import com.vmturbo.common.protobuf.common.CloudTypeEnum;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Conversion class for {@link CloudType}.
 */
public class CloudTypeMapper {

    private static final Map<String, CloudType> probeTypeToCloudType =
            new ImmutableMap.Builder<String, CloudType>()
                    .put(SDKProbeType.AWS.getProbeType(), CloudType.AWS)
                    .put(SDKProbeType.AWS_COST.getProbeType(), CloudType.AWS)
                    .put(SDKProbeType.AWS_BILLING.getProbeType(), CloudType.AWS)
                    .put(SDKProbeType.AWS_LAMBDA.getProbeType(), CloudType.AWS)
                    .put(SDKProbeType.AZURE.getProbeType(), CloudType.AZURE)
                    .put(SDKProbeType.AZURE_EA.getProbeType(), CloudType.AZURE)
                    .put(SDKProbeType.AZURE_SERVICE_PRINCIPAL.getProbeType(), CloudType.AZURE)
                    .put(SDKProbeType.AZURE_STORAGE_BROWSE.getProbeType(), CloudType.AZURE)
                    .put(SDKProbeType.AZURE_COST.getProbeType(), CloudType.AZURE)
                    .put(SDKProbeType.APPINSIGHTS.getProbeType(), CloudType.AZURE)
                    .put(SDKProbeType.GCP.getProbeType(), CloudType.GCP)
                    .put(SDKProbeType.GCP_COST.getProbeType(), CloudType.GCP)
                    .put(SDKProbeType.GCP_SERVICE_ACCOUNT.getProbeType(), CloudType.GCP)
                    .put(SDKProbeType.GCP_PROJECT.getProbeType(), CloudType.GCP)
                    .put(SDKProbeType.GCP_BILLING.getProbeType(), CloudType.GCP)
                    .build();

    private static final BiMap<CloudTypeEnum.CloudType, CloudType> CLOUD_TYPE_MAPPINGS =
            new ImmutableBiMap.Builder<CloudTypeEnum.CloudType, CloudType>()
                    .put(CloudTypeEnum.CloudType.UNKNOWN_CLOUD, CloudType.UNKNOWN)
                    .put(CloudTypeEnum.CloudType.AWS, CloudType.AWS)
                    .put(CloudTypeEnum.CloudType.AZURE, CloudType.AZURE)
                    .put(CloudTypeEnum.CloudType.GCP, CloudType.GCP)
                    .put(CloudTypeEnum.CloudType.HYBRID_CLOUD, CloudType.HYBRID)
            .build();

    /**
     * Get Cloud type from target type.
     *
     * @param targetType Target type (probe type).
     * @return Cloud type or null if not found.
     */
    @Nonnull
    public Optional<CloudType> fromTargetType(@Nonnull final String targetType) {
        return Optional.ofNullable(probeTypeToCloudType.get(targetType));
    }

    /**
     * Converts the internal XL cloud type protobuf enum to the corresponding api one.
     *
     * @param cloudType the {@link CloudTypeEnum.CloudType} to convert.
     * @return the associated {@link CloudType}, or {@link CloudType#UNKNOWN}
     */
    @Nonnull
    public static CloudType fromXlProtoEnumToApi(final CloudTypeEnum.CloudType cloudType) {
        return CLOUD_TYPE_MAPPINGS.getOrDefault(cloudType, CloudType.UNKNOWN);
    }

    /**
     * Converts the api cloud type enum to the corresponding internal XL protobuf one.
     *
     * @param cloudType the {@link CloudType} to convert.
     * @return the associated {@link CloudTypeEnum.CloudType}, or
     *         {@link CloudTypeEnum.CloudType#UNKNOWN_CLOUD}
     */
    @Nonnull
    public static CloudTypeEnum.CloudType fromApiToXlProtoEnum(final CloudType cloudType) {
        return CLOUD_TYPE_MAPPINGS.inverse().getOrDefault(
                cloudType, CloudTypeEnum.CloudType.UNKNOWN_CLOUD);
    }
}
