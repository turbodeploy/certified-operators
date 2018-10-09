package com.vmturbo.mediation.cloud.converter;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import com.vmturbo.mediation.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.cloud.IEntityConverter;
import com.vmturbo.mediation.hybrid.cloud.utils.OSType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ComputeTierData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.ProfileDTO.CommodityProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.VMProfileDTO;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * The compute tier converter. COMPUTE_TIER comes from vm profile.
 *
 * Connect ComputeTier to Region based on license info in profile dto. For AWS, those compute
 * tiers which are referenced in VM dto, are connected to AZ in {@link VirtualMachineConverter}.
 *
 * CLOUD-TODO: figure out how to store info from EntityProfileDTO to EntityDTO
 * this is a general task for how to store entity specific information in XL
 */
public class ComputeTierConverter implements IEntityConverter {

    private SDKProbeType probeType;

    public ComputeTierConverter(@Nonnull SDKProbeType probeType) {
        this.probeType = probeType;
    }

    @Override
    public boolean convert(@Nonnull EntityDTO.Builder entity, @Nonnull CloudDiscoveryConverter converter) {
        String computeTierId = entity.getId();
        EntityProfileDTO profileDTO = converter.getProfileDTO(computeTierId);
        VMProfileDTO vmProfileDTO = profileDTO.getVmProfileDTO();

        // set compute tier specific data
        entity.setComputeTierData(ComputeTierData.newBuilder()
                .setFamily(vmProfileDTO.getFamily())
                .setDedicatedStorageNetworkState(vmProfileDTO.getDedicatedStorageNetworkState())
                .build());

        // connect CT to Region, based on license
        profileDTO.getVmProfileDTO().getLicenseList().forEach(licenseMapEntry ->
                entity.addLayeredOver(licenseMapEntry.getRegion()));

        // connect CT to ST, based on diskType in VM profile
        String diskType = profileDTO.getVmProfileDTO().getDiskType();

        if (probeType == SDKProbeType.AWS) {
            // AWS: diskType can be ssd, hdd, ebs
            if ("ebs".equalsIgnoreCase(diskType)) {
                // connect to all AWS ebs StorageTiers
                converter.getAllStorageTierIds().forEach(storageTierId ->
                        entity.addLayeredOver(storageTierId));
            } else {
                // disk type may be unknown, we don't know which ST to connect to in this case
                if (!"UNKNOWN".equalsIgnoreCase(diskType)) {
                    entity.addLayeredOver(converter.getStorageTierId(diskType.toUpperCase()));
                }
            }
        } else if (probeType == SDKProbeType.AZURE) {
            // Azure: diskType can be SSD, HDD
            // only STANDARD supports HDD, PREMIUM supports both SSD and HDD
            // connect SSD to managed_standard, managed_premium, unmanaged_standard, unmanaged_premium
            // connect HDD to managed_standard, unmanaged_standard,
            converter.getAllStorageTierIds().forEach(stId -> {
                if ("SSD".equalsIgnoreCase(diskType) ||
                        ("HDD".equalsIgnoreCase(diskType) && stId.contains("STANDARD"))) {
                    entity.addLayeredOver(stId);
                }
            });
        }

        // create sold commodities, based on commodityProfile defined in entityProfile
        entity.addAllCommoditiesSold(createComputeTierSoldCommodities(profileDTO));

        // owned by cloud service
        converter.ownedByCloudService(EntityType.COMPUTE_TIER, computeTierId);

        return true;
    }

    /**
     * Create a list of sold commodities based on the commodityProfile list and attributes inside
     * the vm profile DTO. Currently, following commodities are created:
     *     CPU,
     *     CPU_PROVISIONED,
     *     MEM,
     *     MEM_PROVISIONED,
     *     IO_THROUGHPUT,
     *     NET_THROUGHPUT,
     *     NUM_DISK (AWS doesn't have it, so capacity will be 0)
     *     LICENSE_ACCESS
     *
     * @param profileDTO the EntityProfileDTO based on which to create sold commodities
     * @return list of sold commodities for the compute tier
     */
    private List<CommodityDTO> createComputeTierSoldCommodities(@Nonnull EntityProfileDTO profileDTO) {
        List<CommodityDTO> soldCommodities = Lists.newArrayList();

        float memorySize = 0.0f;
        float ioThroughputSize = 0.0f;
        float netThroughputSize = 0.0f;
        float numDiskSize = 0.0f;

        for (CommodityProfileDTO commodityProfileDTO : profileDTO.getCommodityProfileList()) {
            CommodityType commodityType = commodityProfileDTO.getCommodityType();
            float capacity = commodityProfileDTO.getCapacity();
            if (commodityType == CommodityType.VMEM) {
                memorySize = capacity;
            } else if (commodityType == CommodityType.IO_THROUGHPUT) {
                ioThroughputSize = capacity;
            } else if (commodityType == CommodityType.NET_THROUGHPUT) {
                netThroughputSize = capacity;
            } else if (commodityType == CommodityType.NUM_DISK) {
                numDiskSize = capacity;
            }
        }

        VMProfileDTO vmProfileDTO = profileDTO.getVmProfileDTO();

        // CPU
        float totalCpuSold = vmProfileDTO.getVCPUSpeed() * vmProfileDTO.getNumVCPUs();
        soldCommodities.add(CommodityDTO.newBuilder()
                .setCommodityType(CommodityType.CPU)
                .setCapacity(totalCpuSold)
                .build());

        // CPU_PROVISIONED
        soldCommodities.add(CommodityDTO.newBuilder()
                .setCommodityType(CommodityType.CPU_PROVISIONED)
                .setCapacity(totalCpuSold)
                .build());

        // MEM
        soldCommodities.add(CommodityDTO.newBuilder()
                .setCommodityType(CommodityType.MEM)
                .setCapacity(memorySize)
                .build());

        // MEM_PROVISIONED
        soldCommodities.add(CommodityDTO.newBuilder()
                .setCommodityType(CommodityType.MEM_PROVISIONED)
                .setCapacity(memorySize)
                .build());

        // IO_THROUGHPUT
        soldCommodities.add(CommodityDTO.newBuilder()
                .setCommodityType(CommodityType.IO_THROUGHPUT)
                .setCapacity(ioThroughputSize)
                .build());

        // NET_THROUGHPUT
        soldCommodities.add(CommodityDTO.newBuilder()
                .setCommodityType(CommodityType.NET_THROUGHPUT)
                .setCapacity(netThroughputSize)
                .build());

        // NUM_DISK
        soldCommodities.add(CommodityDTO.newBuilder()
                .setCommodityType(CommodityType.NUM_DISK)
                .setCapacity(numDiskSize)
                .build());

        // LICENSE_ACCESS
        // todo: currently we collect all licenses for all regions and add commodity for each
        // distinct license, we may need to change it if we want to store different licenses for
        // different regions in some field in EntityDTO
        profileDTO.getVmProfileDTO().getLicenseList().stream()
                .flatMap(licenseMapEntry -> licenseMapEntry.getLicenseNameList().stream())
                .distinct()
                .forEach(license -> soldCommodities.add(
                        CommodityDTO.newBuilder()
                                .setCommodityType(CommodityType.LICENSE_ACCESS)
                                .setKey(OSType.lookupByPattern(Optional.ofNullable(license)).getName())
                                .build()));

        return soldCommodities;
    }
}
