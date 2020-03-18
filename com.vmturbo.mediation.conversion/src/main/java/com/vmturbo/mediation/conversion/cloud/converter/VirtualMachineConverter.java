package com.vmturbo.mediation.conversion.cloud.converter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.mediation.conversion.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.conversion.cloud.IEntityConverter;
import com.vmturbo.mediation.hybrid.cloud.common.OsDetailParser;
import com.vmturbo.mediation.hybrid.cloud.common.OsType;
import com.vmturbo.mediation.hybrid.cloud.common.PropertyName;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.InstanceDiskType;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.VMProfileDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * CloudDiscoveryConverter for Cloud VMs. The commodities bought will be shifted from Availability
 * Zones and Storages to Compute Tiers and Storage Tiers respectively. The VMs will also be
 * connected to the appropriate AZ or Region.
 */
public class VirtualMachineConverter implements IEntityConverter {

    // set of access commodity types that should be removed from bought commodities list
    private static final Set<CommodityType> ACCESS_COMMODITY_TYPES_TO_REMOVE = ImmutableSet.of(
            CommodityType.DSPM_ACCESS,
            CommodityType.DATACENTER,
            CommodityType.DATASTORE,
            CommodityType.VMPM_ACCESS
    );

    // set of commodities that the active field should be cleared (set back to true by default)
    public static final Set<CommodityType> COMMODITIES_TO_CLEAR_ACTIVE = ImmutableSet.of(
            CommodityType.MEM_PROVISIONED,
            CommodityType.CPU_PROVISIONED
    );

    private static final String VM_DEFAULT_TENANCY = "default";
    private static final String VM_DEDICATED_TENANCY = "dedicated";
    private static final String VM_HOST_TENANCY = "host";
    private static final Map<String, Tenancy> tenancyMapping = ImmutableMap.of(
        VM_DEFAULT_TENANCY, Tenancy.DEFAULT,
        VM_DEDICATED_TENANCY, Tenancy.DEDICATED,
        VM_HOST_TENANCY, Tenancy.HOST
    );
    private final Logger logger = LogManager.getLogger();

    private SDKProbeType probeType;

    /**
     * Constructor for {@link VirtualMachineConverter}.
     * @param probeType Probe Type
     */
    public VirtualMachineConverter(@Nonnull SDKProbeType probeType) {
        this.probeType = probeType;
    }

    /**
     * Convert the specified entity.
     * @param entity the entity to convert
     * @param converter the {@link CloudDiscoveryConverter} instance which contains all info needed for entity specific converters
     * @return true if conversion successful
     */
    @Override
    public boolean convert(@Nonnull Builder entity, @Nonnull CloudDiscoveryConverter converter) {
        // if the VM doesn't have profileId, then it's a fake VM created for hosting
        // DatabaseServer in AWS. flag this entity for removal
        if (!entity.hasProfileId()) {
            return false;
        }
        VMProfileDTO vmProfileDTO = converter.getProfileDTO(entity.getProfileId()).getVmProfileDTO();

        // find id of az for this VM, which will be used later in volume entity and zone commodity
        Optional<String> azId = converter.getRawEntityDTO(entity.getId())
                .getCommoditiesBoughtList().stream()
                .map(CommodityBought::getProviderId)
                .filter(id -> {
                    EntityDTO provider = converter.getRawEntityDTO(id);
                    if (provider == null) {
                        provider = converter.getRawEntityDTO(converter.getRegionIdFromAzId(id));
                    }
                    final EntityType providerType = provider.getEntityType();
                    return providerType == EntityType.PHYSICAL_MACHINE ||
                        providerType == EntityType.AVAILABILITY_ZONE ||
                        providerType == EntityType.REGION;
                })
                .findAny();

        // new list of CommodityBought bought by this VM
        final List<CommodityBought> newCommodityBoughtList = new ArrayList<>();
        for (CommodityBought commodityBought : entity.getCommoditiesBoughtList()) {
            CommodityBought.Builder cbBuilder = commodityBought.toBuilder();
            // filter out access commodities
            cbBuilder.clearBought();
            cbBuilder.addAllBought(commodityBought.getBoughtList()
                    .stream()
                    .filter(commodityDTO -> !ACCESS_COMMODITY_TYPES_TO_REMOVE.contains(commodityDTO.getCommodityType()))
                    // TODO: we will require a setting where for AWS, we can enable diskType and diskSize
                    //       if for a particular entity, this setting is true in policy tab.
                    .filter(commodityDTO -> commodityDTO.getCommodityType() != CommodityType.INSTANCE_DISK_SIZE ||
                            vmProfileDTO.hasInstanceDiskSize())
                    .filter(commodityDTO -> commodityDTO.getCommodityType() != CommodityType.INSTANCE_DISK_TYPE ||
                            (vmProfileDTO.hasInstanceDiskType() &&
                                    vmProfileDTO.getInstanceDiskType() != InstanceDiskType.NONE))
                    .map(commodityDTO ->
                            // clear active field (active is true by default) for some
                            // commodities since they are set to false in probe, we can't
                            // change probe since it will affect classic
                            COMMODITIES_TO_CLEAR_ACTIVE.contains(commodityDTO.getCommodityType()) ?
                                    commodityDTO.toBuilder().clearActive().build() : commodityDTO)
                    .collect(Collectors.toList()));

            // change provider
            String providerId = commodityBought.getProviderId();
            EntityDTO provider = converter.getRawEntityDTO(providerId);
            if (provider == null) {
                provider = converter.getRawEntityDTO(converter.getRegionIdFromAzId(providerId));
            }
            EntityType providerEntityType = provider.getEntityType();

            // check entity type of original provider defined in unmodified EntityDTO
            if (providerEntityType == EntityType.PHYSICAL_MACHINE ||
                    providerEntityType == EntityType.AVAILABILITY_ZONE ||
                    providerEntityType == EntityType.REGION) {
                if (probeType == SDKProbeType.AWS || probeType == SDKProbeType.GCP) {
                    // connect to AZ
                    entity.addLayeredOver(providerId);
                } else if (probeType == SDKProbeType.AZURE) {
                    // connect to Region
                    entity.addLayeredOver(converter.getRegionIdFromAzId(providerId));
                }

                // buy License_Access commodity
                OsType osType = OsDetailParser.parseOsType(entity.getVirtualMachineData()
                    .getGuestName());
                cbBuilder.addBought(CommodityDTO.newBuilder()
                        .setCommodityType(CommodityType.LICENSE_ACCESS)
                        .setKey(osType.getName())
                        .build());
                Optional<EntityProperty> tenancyProp = entity.getEntityPropertiesList().stream()
                    .filter(prop -> prop.getName().equals(PropertyName.TENANCY)).findFirst();
                if (tenancyProp.isPresent()) {
                    Tenancy tenancy = tenancyMapping.get(tenancyProp.get().getValue());
                    if (tenancy != null) {
                        cbBuilder.addBought(CommodityDTO.newBuilder()
                            .setCommodityType(CommodityType.TENANCY_ACCESS)
                            .setKey(tenancy.toString())
                            .build());
                    } else {
                        logger.error("Unsupported tenancy - {} - found for VM {}",
                            tenancyProp.get().getValue(), entity.getDisplayName());
                    }
                }

                // change commodity provider from AZ to CT
                cbBuilder.setProviderId(entity.getProfileId());
                cbBuilder.setProviderType(EntityType.COMPUTE_TIER);
            } else if (providerEntityType == EntityType.STORAGE_TIER) {
                //  add connected relationship between vm and volume.
                if (commodityBought.hasSubDivision()) {
                    // connect vm to volume
                    String svId = commodityBought.getSubDivision().getSubDivisionId();
                    if (!entity.getLayeredOverList().contains(svId)) {
                        entity.addLayeredOver(svId);
                    }
                }
            }
            newCommodityBoughtList.add(cbBuilder.build());
        }
        azId.ifPresent(az -> {
            if (probeType == SDKProbeType.AWS) {
                connectEphemeralVolumes(entity, vmProfileDTO, az, converter);
            }
        });

        // for AWS or GCP, create a new CommodityBought for VM which buys ZONE access commodity from
        // AZ, and the commodity key is AZ id
        if (probeType == SDKProbeType.AWS || probeType == SDKProbeType.GCP) {
            azId.ifPresent(az -> newCommodityBoughtList.add(CommodityBought.newBuilder()
                    .addBought(CommodityDTO.newBuilder()
                            .setCommodityType(CommodityType.ZONE)
                            .setKey(az)
                            .build())
                    .setProviderId(az)
                    .setProviderType(EntityType.AVAILABILITY_ZONE)
                    .build())
            );
        }

        // set new commodities bought
        entity.clearCommoditiesBought();
        entity.addAllCommoditiesBought(newCommodityBoughtList);

        // VM owned by business account
        converter.ownedByBusinessAccount(entity.getId());

        return true;
    }

    /**
     * Connects an ephemeral volume it to the respective storage and zone for every instance store
     * in an AWS virtual machine.
     *
     * @param entity the virtual machine attached to instance stores.
     * @param vmProfileDTO the underlying profile DTO.
     * @param zone  the virtual machine zone
     * @param converter the cloudDiscovery converter
     */
    private void connectEphemeralVolumes(final Builder entity,
                                         final VMProfileDTO vmProfileDTO,
                                         final String zone,
                                         final CloudDiscoveryConverter converter) {
        if (entity.hasVirtualMachineData()) {
            int numInstanceStores = entity.getVirtualMachineData().getNumEphemeralStorages();
            String diskType = vmProfileDTO.getInstanceDiskType().toString();
            String storageTierId = converter.getStorageTierId(diskType);
            for (int i = 0; i < numInstanceStores; i++) {
                String vId = converter.createEphemeralVolumeId(entity.getId(), i, diskType);
                Builder volume = converter.getNewEntityBuilder(vId);
                addLayeredOver(entity, vId);
                addLayeredOver(volume, zone);
                addLayeredOver(volume, storageTierId);
                converter.ownedByBusinessAccount(vId);
            }
        }
    }

    private static void addLayeredOver(final Builder entity, final String layeredOver) {
        if (!entity.getLayeredOverList().contains(layeredOver)) {
            entity.addLayeredOver(layeredOver);
        }
    }
}
