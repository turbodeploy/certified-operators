package com.vmturbo.mediation.azure;

import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.conversion.cloud.converter.ComputeTierConverter;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.InstanceDiskType;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.VMProfileDTO;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 *  Converter class to convert Azure VM profiles to ComputeTiers.
 */
public class AzureComputeTierConverter extends ComputeTierConverter {

    /**
     * Constructor for {@link AzureComputeTierConverter}.
     */
    public AzureComputeTierConverter() {
        super(SDKProbeType.AZURE);
    }

    @Override
    protected List<CommodityDTO> createComputeTierSoldCommodities(@Nonnull EntityProfileDTO profileDTO) {
        List<CommodityDTO> soldCommodities = super.createComputeTierSoldCommodities(profileDTO);
        VMProfileDTO vmProfileDTO = profileDTO.getVmProfileDTO();

        // INSTANCE_DISK_TYPE
        if (vmProfileDTO.hasInstanceDiskType() &&
                vmProfileDTO.getInstanceDiskType() != InstanceDiskType.NONE) {
            soldCommodities.add(createCommodityDTO(
                    // Set the capacity of INSTANCE_DISK_TYPE to the enum number.
                    // Then an entity can only be moved to a better disk.
                    CommodityType.INSTANCE_DISK_TYPE, vmProfileDTO.getInstanceDiskType().getNumber()));
        }

        // INSTANCE_DISK_SIZE
        if (vmProfileDTO.hasInstanceDiskSize()) {
            soldCommodities.add(createCommodityDTO(
                    CommodityType.INSTANCE_DISK_SIZE, vmProfileDTO.getInstanceDiskSize()));
        }
        return soldCommodities;
    }

}
