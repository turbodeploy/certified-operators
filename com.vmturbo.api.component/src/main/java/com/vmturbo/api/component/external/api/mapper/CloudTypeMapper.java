package com.vmturbo.api.component.external.api.mapper;

import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.enums.CloudType;
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
                    .put(SDKProbeType.AZURE.getProbeType(), CloudType.AZURE)
                    .put(SDKProbeType.AZURE_EA.getProbeType(), CloudType.AZURE)
                    .put(SDKProbeType.GCP.getProbeType(), CloudType.GCP)
                    .build();

    /**
     * Get Cloud type from target type.
     *
     * @param targetType Target type (probe type).
     * @return Cloud type or null if not found.
     */
    public CloudType fromTargetType(@Nonnull final String targetType) {
        return probeTypeToCloudType.get(targetType);
    }
}
