package com.vmturbo.mediation.conversion.cloud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import com.vmturbo.mediation.conversion.util.CloudService;
import com.vmturbo.platform.common.builders.CommodityBuilders;
import com.vmturbo.platform.common.builders.EntityBuilders;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.VMProfileDTO;

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
        final CloudProviderConversionContext mockContext = mock(CloudProviderConversionContext.class);
        when(mockContext.getCloudEntityTypeForProfileType(any()))
            .thenReturn(Optional.of(EntityType.COMPUTE_TIER));
        when(mockContext.getVolumeIdFromStorageFilePath(any(), any()))
            .thenReturn(Optional.of("volumeId"));
        final String storageTierId = "NVME_SSD";
        when(mockContext.getStorageTierId(any()))
            .thenReturn(storageTierId);

        final CloudDiscoveryConverter converter = new CloudDiscoveryConverter(
            DiscoveryResponse.getDefaultInstance(), mockContext);

        final int vmProfileSize = 12345;
        final String vmProfileId = "vmProfileId";
        final EntityDTO vm = EntityBuilders.virtualMachine("vmId")
            .profileId(vmProfileId)
            .numEphemeralStorages(1)
            .buying(CommodityBuilders.cpuMHz().from("zoneId", EntityType.PHYSICAL_MACHINE))
            .build();
        final EntityProfileDTO vmProfile = EntityProfileDTO.newBuilder()
            .setId(vmProfileId)
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setVmProfileDTO(VMProfileDTO.newBuilder().setInstanceDiskSize(vmProfileSize))
            .build();
        converter.createEntityDTOFromProfile(vmProfile);
        converter.preProcessEntityDTO(vm);

        final List<Builder> result = converter.getNewEntitiesGroupedByType().get(EntityType.VIRTUAL_VOLUME);
        assertEquals(1, result.size());
        final VirtualVolumeData resultData = result.get(0).getVirtualVolumeData();
        assertTrue(resultData.getIsEphemeral());
        assertEquals(vmProfileSize, resultData.getStorageAmountCapacity(), DELTA);
        assertEquals(AttachmentState.ATTACHED, resultData.getAttachmentState());

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
