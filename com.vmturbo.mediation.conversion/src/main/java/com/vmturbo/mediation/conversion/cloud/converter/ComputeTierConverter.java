package com.vmturbo.mediation.conversion.cloud.converter;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.vmturbo.mediation.conversion.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.conversion.cloud.IEntityConverter;
import com.vmturbo.mediation.hybrid.cloud.common.OsDetailParser;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ComputeTierData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.ProfileDTO.CommodityProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.VMProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.VMProfileDTO.TenancyType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * The compute tier converter. COMPUTE_TIER comes from vm profile.
 *<p></p>
 * Connect ComputeTier to Region based on license info in profile dto. For AWS, those compute
 * tiers which are referenced in VM dto, are connected to AZ in {@link VirtualMachineConverter}.
 * <p></p>
 * CLOUD-TODO: figure out how to store info from EntityProfileDTO to EntityDTO
 * this is a general task for how to store entity specific information in XL
 */
public class ComputeTierConverter implements IEntityConverter {

    private SDKProbeType probeType;

    private static final Map<SDKProbeType, List<CommodityType>> resizableCommodities =
            ImmutableMap.of(SDKProbeType.AWS,
                    ImmutableList.of(CommodityType.IO_THROUGHPUT, CommodityType.NET_THROUGHPUT));

    private static final Map<TenancyType, Tenancy> tenancyMapping = ImmutableMap.of(
        TenancyType.SHARED, Tenancy.DEFAULT,
        TenancyType.DEDICATED, Tenancy.DEDICATED,
        TenancyType.HOST, Tenancy.HOST
    );

    /**
     * Constructor for {@link ComputeTierConverter}.
     * @param probeType Probe Type
     */
    public ComputeTierConverter(@Nonnull SDKProbeType probeType) {
        this.probeType = probeType;
    }

    @Override
    public boolean convert(@Nonnull EntityDTO.Builder entity, @Nonnull CloudDiscoveryConverter converter) {
        String computeTierId = entity.getId();
        EntityProfileDTO profileDTO = converter.getProfileDTO(computeTierId);
        VMProfileDTO vmProfileDTO = profileDTO.getVmProfileDTO();

        // set entity properties
        entity.addAllEntityProperties(profileDTO.getEntityPropertiesList());

        // set compute tier specific data
        final ComputeTierData.Builder computeTierDataBuilder = ComputeTierData.newBuilder()
                .setFamily(vmProfileDTO.getInstanceSizeFamily())
                .setQuotaFamily(vmProfileDTO.getQuotaFamily())
                .setDedicatedStorageNetworkState(vmProfileDTO.getDedicatedStorageNetworkState())
                .setNumCoupons(vmProfileDTO.getNumberOfCoupons())
                .setNumCores(vmProfileDTO.getNumVCPUs());
        if (vmProfileDTO.hasInstanceDiskSize()) {
            computeTierDataBuilder.setInstanceDiskSizeGb(vmProfileDTO.getInstanceDiskSize());
        }
        if (vmProfileDTO.hasInstanceDiskType()) {
            computeTierDataBuilder.setInstanceDiskType(vmProfileDTO.getInstanceDiskType());
        }
        if (vmProfileDTO.hasNumInstanceDisks()) {
            computeTierDataBuilder.setNumInstanceDisks(vmProfileDTO.getNumInstanceDisks());
        }
        entity.setComputeTierData(computeTierDataBuilder.build());

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
        } else if (probeType == SDKProbeType.GCP) {
            // GCP: diskType can be Standard, SSD
            converter.getAllStorageTierIds().forEach(stId -> {
                    entity.addLayeredOver(stId);
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
    protected List<CommodityDTO> createComputeTierSoldCommodities(@Nonnull EntityProfileDTO profileDTO) {
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
        soldCommodities.add(createCommodityDTO(CommodityType.CPU, totalCpuSold));

        // CPU_PROVISIONED
        soldCommodities.add(createCommodityDTO(CommodityType.CPU_PROVISIONED, totalCpuSold));

        // MEM
        soldCommodities.add(createCommodityDTO(CommodityType.MEM, memorySize));

        // MEM_PROVISIONED
        soldCommodities.add(createCommodityDTO(CommodityType.MEM_PROVISIONED, memorySize));

        // IO_THROUGHPUT
        soldCommodities.add(createCommodityDTO(CommodityType.IO_THROUGHPUT, ioThroughputSize));

        // NET_THROUGHPUT
        soldCommodities.add(createCommodityDTO(CommodityType.NET_THROUGHPUT, netThroughputSize));

        // NUM_DISK
        soldCommodities.add(createCommodityDTO(CommodityType.NUM_DISK, numDiskSize));

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
                                .setKey(OsDetailParser.parseOsType(license).getName())
                                .build()));
        // Add tenancy access commodities sold
        vmProfileDTO.getTenancyList().stream()
            .map(tenancyType -> tenancyMapping.get(tenancyType))
            .forEach(tenancy -> soldCommodities.add(
                CommodityDTO.newBuilder()
                    .setCommodityType(CommodityType.TENANCY_ACCESS)
                    .setKey(tenancy.toString()).build()));
        return soldCommodities;
    }

    /**
     * Create a {@link CommodityDTO} using commodityType and capacity.
     *
     * @param commodityType the type of {@link CommodityDTO}
     * @param capacity the capacity of {@link CommodityDTO}
     * @return {@link CommodityDTO}
     */
    protected CommodityDTO createCommodityDTO(CommodityType commodityType, double capacity) {
        final Builder commodityBuilder = CommodityDTO.newBuilder()
                .setCommodityType(commodityType)
                .setCapacity(capacity);
        final List<CommodityType> commTypes = resizableCommodities.get(probeType);
        if (commTypes != null && commTypes.contains(commodityType)) {
            commodityBuilder.setResizable(true);
        }
        return  commodityBuilder.build();
    }
}
