package com.vmturbo.mediation.azure.volumes;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.cloud.CloudDiscoveryConverter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

/**
 * Subclass of CloudDiscoveryConverter that makes some changes to how Storages are processed in
 * the Azure Volume Converter.  We create Regions in the preprocess stage since we do not have
 * entityDTOs for them but must infer their ids from the information in the Storage DTO.
 */
public class AzureVolumesCloudDiscoveryConverter extends CloudDiscoveryConverter {

    public AzureVolumesCloudDiscoveryConverter(@Nonnull DiscoveryResponse discoveryResponse,
                                               @Nonnull AzureVolumesConversionContext conversionContext) {
        super(discoveryResponse, conversionContext);
    }

    /**
     * The azure volumes probe only gives us Storages, but in addition to creating the volumes
     * from the storages, we need to create the regions that the volumes are associated with.
     * We do this in the preprocessing stage.
     *
     * @param entityDTO the EntityDTO to pre process
     */
    @Override
    protected void preProcessEntityDTO(@Nonnull final EntityDTO entityDTO) {
        super.preProcessEntityDTO(entityDTO);
        if (entityDTO.getEntityType() == EntityType.STORAGE) {
            addRegionsUsedByStorage(entityDTO);
        }
    }

    /**
     * If this storage entity DTO has availability zone information in it, extract the region ID
     * and add a DTO for the region to the set of DTOs we are generating.
     *
     * @param entityDTO the {@link EntityDTO} for a Storage.
     */
    private void addRegionsUsedByStorage(@Nonnull final EntityDTO entityDTO) {
        Objects.requireNonNull(entityDTO);
        getAvailabilityZone(entityDTO.toBuilder())
            .ifPresent(azId -> {
                String regionId = getRegionIdFromAzId(azId);
                newEntityBuildersById.computeIfAbsent(
                    regionId, k -> EntityDTO.newBuilder()
                        .setEntityType(EntityType.REGION)
                        .setId(regionId)
                        .setDisplayName(getRegionNameFromAzId(azId)));
            });
    }
}

