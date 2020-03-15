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

import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import com.vmturbo.mediation.conversion.util.CloudService;
import com.vmturbo.platform.common.builders.CommodityBuilders;
import com.vmturbo.platform.common.builders.EntityBuilders;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
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
}
