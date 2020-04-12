package com.vmturbo.mediation.gcp;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.conversion.cloud.CloudProviderConversionContext;
import com.vmturbo.mediation.conversion.cloud.IEntityConverter;
import com.vmturbo.mediation.conversion.cloud.converter.VirtualMachineConverter;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * The conversion context provided by GCP probe which contains logic specific to GCP.
 */
public class GcpConversionContext implements CloudProviderConversionContext {

    // converters for different entity types
    private static final Map<EntityType, IEntityConverter> GCP_ENTITY_CONVERTERS;
    static {
        final Map<EntityType, IEntityConverter> converters = new EnumMap<>(EntityType.class);
        converters.put(EntityType.VIRTUAL_MACHINE, new VirtualMachineConverter(SDKProbeType.GCP));
        GCP_ENTITY_CONVERTERS = Collections.unmodifiableMap(converters);
    }

    @Nonnull
    @Override
    public Map<EntityType, IEntityConverter> getEntityConverters() {
        return GCP_ENTITY_CONVERTERS;
    }

    @Nonnull
    @Override
    public String getStorageTierId(@Nonnull String storageTierName) {
        return "gcp::ST::" + storageTierName;
    }

    /**
     * Get region id based on AZ id. It gets "ca-central-1" from AZ id and then combine
     * with prefix into the Region id. For example:
     *     GCP AZ id is:     GCP::ca-central-1::PM::ca-central-1b
     *     GCP Region id is: GCP::ca-central-1::DC::ca-central-1b
     *
     * @param azId id of the AZ
     * @return region id for the AZ
     */
    @Nonnull
    @Override
    public String getRegionIdFromAzId(@Nonnull String azId) {
        String region = azId.split("::", 3)[1];
        return "gcp::" + region + "::DC::" + region;
    }

}
