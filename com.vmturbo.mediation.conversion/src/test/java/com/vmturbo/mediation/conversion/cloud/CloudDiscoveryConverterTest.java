package com.vmturbo.mediation.conversion.cloud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.mediation.conversion.util.CloudService;
import com.vmturbo.platform.common.builders.CommodityBuilders;
import com.vmturbo.platform.common.builders.EntityBuilders;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.SubDivisionData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VirtualMachineFileDescriptor;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineFileType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.VMProfileDTO;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;

/**
 * Todo: here's hoping that someday soon the conversion logic is absorbed into the probes, and
 * this test is no longer necessary. We can dream, right?
 */
public class CloudDiscoveryConverterTest {

    private static final double DELTA = 0.001;

    /**
     * Test that when a volume entity is generated from a VM's ephemeral storage, its storage
     * amount capacity is derived from the VM's profile. Also that its state is always Attached.
     */
    @Test
    public void testEphemeralVolumeSize() {
        final int vmProfileSizeInGB = 1900;
        final String vmProfileId = "vmProfileId";
        final String vmId = "vmId";
        final EntityDTO.InstanceDiskType vmDiskType = EntityDTO.InstanceDiskType.NVME_SSD;

        final CloudProviderConversionContext mockContext = mock(CloudProviderConversionContext.class);
        final CloudDiscoveryConverter converter = new CloudDiscoveryConverter(
            DiscoveryResponse.getDefaultInstance(), mockContext);
        final String volumeId = converter.createEphemeralVolumeId(vmId, 0, vmDiskType.toString());
        when(mockContext.getCloudEntityTypeForProfileType(any()))
                .thenReturn(Optional.of(EntityType.COMPUTE_TIER));
        final String storageTierId = "NVME_SSD";
        when(mockContext.getStorageTierId(any()))
                .thenReturn(storageTierId);
        when(mockContext.getAvailabilityZone(any())).thenReturn(Optional.of("azure::us_east"));

        when(mockContext.getVolumeIdFromStorageFilePath(any(), any()))
                .thenReturn(Optional.of(volumeId));


        final EntityDTO vm = EntityBuilders.virtualMachine("vmId")
            .profileId(vmProfileId)
            .numEphemeralStorages(1)
            .buying(CommodityBuilders.cpuMHz().from("zoneId", EntityType.PHYSICAL_MACHINE))
            .build();
        final EntityProfileDTO vmProfile = EntityProfileDTO.newBuilder()
            .setId(vmProfileId)
            .setEntityType(EntityType.VIRTUAL_MACHINE)
                .setVmProfileDTO(VMProfileDTO.newBuilder()
                        .setInstanceDiskType(vmDiskType)
                        .setInstanceDiskSize(vmProfileSizeInGB))
            .build();
        final EntityProperty localName = EntityProperty.newBuilder()
                .setNamespace(SupplyChainConstants.NAMESPACE)
                .setName(SupplyChainConstants.LOCAL_NAME)
                .setValue(volumeId)
                .build();
        final EntityDTO storage = EntityBuilders.storage("storageId")
                .file("path", 100L, 1L, VirtualMachineFileType.DISK, 0, "", "", false, "", "", "",
                        Collections.singletonList(localName), volumeId)
                .build();
        converter.createEntityDTOFromProfile(vmProfile);
        converter.preProcessEntityDTO(vm);
        converter.preProcessEntityDTO(storage);

        final List<Builder> result = converter.getNewEntitiesGroupedByType().get(EntityType.VIRTUAL_VOLUME);
        assertEquals(1, result.size());
        final EntityDTO.Builder volume = result.get(0);
        final VirtualVolumeData resultData = volume.getVirtualVolumeData();
        assertTrue(resultData.getIsEphemeral());
        assertEquals(vmProfileSizeInGB * (1000f * 1000f * 1000f) / 1024.0 / 1024.0, resultData.getStorageAmountCapacity(), DELTA);
        assertEquals(AttachmentState.ATTACHED, resultData.getAttachmentState());
        final List<EntityProperty> volumeEntityProperties = volume.getEntityPropertiesList();
        assertFalse(volumeEntityProperties.isEmpty());
        assertEquals(volumeId, volumeEntityProperties.get(0).getValue());
        assertEquals(SupplyChainConstants.LOCAL_NAME, volumeEntityProperties.get(0).getName());
        final Builder storageTier = converter.getNewEntityBuilder(storageTierId);
        assertNotNull(storageTier);
        assertEquals(EntityType.STORAGE_TIER, storageTier.getEntityType());
        assertEquals(storageTierId, storageTier.getId());
    }

    /**
     * Test converting discovery response with a single Service Provider entity.
     */
    @Test
    public void testConvertServiceProvider() {
        // ARRANGE

        // Original discovery response consists of a single Service Provider entity
        final EntityDTO serviceProviderDto = EntityDTO.newBuilder()
                .setEntityType(EntityType.SERVICE_PROVIDER)
                .setId("serviceProviderId")
                .build();
        final DiscoveryResponse response = DiscoveryResponse.newBuilder()
                .addEntityDTO(serviceProviderDto)
                .build();

        // Cloud conversion probe adds one Cloud Service entity
        final CloudProviderConversionContext context = mock(CloudProviderConversionContext.class);
        when(context.getCloudServicesToCreate()).thenReturn(ImmutableSet.of(CloudService.AWS_EBS));

        // ACT
        final CloudDiscoveryConverter converter = new CloudDiscoveryConverter(response, context);
        final DiscoveryResponse convertedResponse = converter.convert();

        // ASSERT

        // Result should contain 2 entities: Service Provider and Cloud Service
        assertEquals(2, convertedResponse.getEntityDTOCount());

        // Get Service Provider and check that Cloud Service was added to ConsistsOf list
        final Optional<EntityDTO> serviceProvider = convertedResponse.getEntityDTOList()
                .stream()
                .filter(e -> e.getEntityType() == EntityType.SERVICE_PROVIDER)
                .findAny();
        assertTrue(serviceProvider.isPresent());
        assertEquals(1, serviceProvider.get().getConsistsOfCount());
        final String consistsOfId = serviceProvider.get().getConsistsOf(0);
        assertEquals(CloudService.AWS_EBS.getId(), consistsOfId);
    }

    /**
     * Test for VHD file attached to a VM, the VV entityDTO should have a attached status.
     */
    @Test
    public void testAzureVHDFileAttachedToVMShouldHasAttachedStatus() {
        // Given
        final String vmId = "azure::VM::8c009224-74d3-4da1-8423-b49b23c39f47";
        final String vmDisplayName = "discoelastic1";
        final String stProviderId = "azure::ST::eastus2-unmanaged_standard";
        final String vhdFilePath = "https://6ajihufzztissstorage.blob.core.windows.net/vhds/os-discoelastic1.vhd";
        final String subDivisionId = "https:::::6ajihufzztissstorage.blob.core.windows.net::vhds::os-discoelastic1.vhd";
        final String vvDisplayName = "os-discoelastic1.vhd";

        final EntityDTO vmDto = createVMEntityDTOWithFile(vmId,
                                                          vmDisplayName,
                                                          stProviderId,
                                                          vhdFilePath,
                                                          subDivisionId,
                                                          vvDisplayName);

        final DiscoveryResponse response = DiscoveryResponse.newBuilder().addEntityDTO(vmDto).build();
        final CloudProviderConversionContext context = mock(CloudProviderConversionContext.class);

        // When
        final CloudDiscoveryConverter converter = new CloudDiscoveryConverter(response, context);
        final DiscoveryResponse convertedResponse = converter.convert();

        // Verify
        assertEquals(2, convertedResponse.getEntityDTOCount());

        final List<EntityDTO> vvDtos = convertedResponse.getEntityDTOList().stream()
            .filter(entityDTO -> entityDTO.getEntityType() == EntityType.VIRTUAL_VOLUME)
            .collect(Collectors.toList());

        assertEquals(1, vvDtos.size());
        final EntityDTO vvDto = vvDtos.get(0);
        assertEquals(EntityType.VIRTUAL_VOLUME, vvDto.getEntityType());
        assertTrue(vvDto.hasVirtualVolumeData());
        assertEquals(AttachmentState.ATTACHED, vvDto.getVirtualVolumeData().getAttachmentState());
    }

    /**
     * Create VM EntityDTO with VirtualMachineData has a file.
     *
     * @param vmId VM id
     * @param vmDisplayName VM displayName
     * @param stProviderId Storage Tier Id
     * @param vhdFilePath full vhd file path
     * @param subDivisionId vhd file path in subDivision form
     * @param vvDisplayName VV DisplayName
     * @return {@link EntityDTO} VM EntityDTO created
     */
    @Nonnull
    private EntityDTO createVMEntityDTOWithFile(@Nonnull final String vmId,
                                                @Nonnull final String vmDisplayName,
                                                @Nonnull final String stProviderId,
                                                @Nonnull final String vhdFilePath,
                                                @Nonnull final String subDivisionId,
                                                @Nonnull final String vvDisplayName) {
        return EntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .setId(vmId)
                .setDisplayName(vmDisplayName)
                .addCommoditiesBought(CommodityBought.newBuilder()
                    .setProviderId(stProviderId)
                    .addBought(CommodityDTO.newBuilder()
                        .setCommodityType(CommodityType.STORAGE_ACCESS)
                    )
                    .addBought(CommodityDTO.newBuilder()
                        .setCommodityType(CommodityType.STORAGE_LATENCY)
                    )
                    .addBought(CommodityDTO.newBuilder()
                        .setCommodityType(CommodityType.STORAGE_LATENCY)
                    )
                    .addBought(CommodityDTO.newBuilder()
                        .setCommodityType(CommodityType.STORAGE_PROVISIONED)
                        .setUsed(30720d)
                        .setActive(false)
                    )
                    .addBought(CommodityDTO.newBuilder()
                        .setCommodityType(CommodityType.STORAGE_AMOUNT)
                        .setUsed(30720d)
                    )
                    .addBought(CommodityDTO.newBuilder()
                        .setCommodityType(CommodityType.STORAGE_CLUSTER)
                        .setKey("Group::UNMANAGED_STANDARD")
                        .setUsed(1d)
                    )
                    .addBought(CommodityDTO.newBuilder()
                        .setCommodityType(CommodityType.DSPM_ACCESS)
                        .setKey("PhysicalMachine::azure::eastus2::PM::eastus2")
                        .setUsed(1d)
                    )
                    .setProviderType(EntityType.STORAGE)
                    .setSubDivision(SubDivisionData.newBuilder()
                        .setSubDivisionId(subDivisionId)
                        .setRedundancyType("LRS")
                        .setStorageAccessCapacity(500f)
                        .setStorageAmountCapacity(30720f)
                        .setDisplayName(vvDisplayName)
                        .build())
                )
                .setVirtualMachineData(VirtualMachineData.newBuilder()
                    .addIpAddress("10.0.0.4")
                    .addFile(VirtualMachineFileDescriptor.newBuilder()
                        .setStorageId(stProviderId)
                        .setPath(vhdFilePath)
                        .setSizeKb(31457280)
                        .setType(VirtualMachineFileType.DISK)
                        .build()
                    )
                    .setNumElasticIps(0)
                )
                .build();
    }

}
