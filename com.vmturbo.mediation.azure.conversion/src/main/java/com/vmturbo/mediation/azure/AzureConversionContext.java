package com.vmturbo.mediation.azure;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.conversion.cloud.CloudProviderConversionContext;
import com.vmturbo.mediation.conversion.cloud.IEntityConverter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * The conversion context provided by Azure probe which contains logic specific to Azure.
 */
public class AzureConversionContext implements CloudProviderConversionContext {

    // converters for different entity types
    private static final Map<EntityType, IEntityConverter> AZURE_ENTITY_CONVERTERS;
    static {
        final Map<EntityType, IEntityConverter> converters = new EnumMap<>(EntityType.class);
        AZURE_ENTITY_CONVERTERS = Collections.unmodifiableMap(converters);
    }

    @Nonnull
    @Override
    public Map<EntityType, IEntityConverter> getEntityConverters() {
        return AZURE_ENTITY_CONVERTERS;
    }

    @Nonnull
    @Override
    public String getStorageTierId(@Nonnull String storageTierName) {
        return "azure::ST::" + storageTierName;
    }

    /**
     * Get region id based on AZ id. It gets region name from AZ id and then combine
     * with prefix into the Region id. For example:
     *     Azure AZ id is:     azure::northcentralus::PM::northcentralus
     *     Azure Region id is: azure::northcentralus::DC::northcentralus
     *
     * @param azId id of the AZ
     * @return region id for the AZ id
     */
    @Nonnull
    @Override
    public String getRegionIdFromAzId(@Nonnull String azId) {
        String region = azId.split("::", 3)[1];
        return "azure::" + region + "::DC::" + region;
    }
}
