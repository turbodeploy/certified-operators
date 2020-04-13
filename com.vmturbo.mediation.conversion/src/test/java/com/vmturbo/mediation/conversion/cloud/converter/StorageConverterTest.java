package com.vmturbo.mediation.conversion.cloud.converter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.Optional;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.vmturbo.mediation.conversion.cloud.CloudDiscoveryConverter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageData.StorageFileDescriptor;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Unit tests for StorageConverterTest.
 */
public class StorageConverterTest {

    @Mock
    CloudDiscoveryConverter cloudDiscoveryConverter;

    private static final String storageId = "storageId";
    private static final String azId = "azure::eastus::PM::eastus";
    private static final String regionId = "azure::eastus::DC::eastus";
    private static final String regionName = "eastus";
    private static final String storageTierId = "azure::ST::MANAGED_STANDARD_SSD";

    /**
     * Test Setup.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }


    /**
     * Test volume {@link com.vmturbo.platform.common.dto.CommonDTO.EntityDTO}'s AnalysisSettings.deletable
     * is false when its {@link com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ConsumerPolicy}.doNotDelete is true.
     * Simulating entity to be an Azure ASR Replica Storage which has its consumer policy wtih doNotDelete be t
     */
    @Test
    public void testConvertWithConsumerPolicyDoNotDeleteTrue() {
        final boolean doNotDelete = true;
        final String volumeId = "::subscriptions::758ad253-cbf5-4b18-8863-3eed0825bf07::resourcegroups::rg_rg::providers::microsoft.compute::disks::rg618_osdisk_1_d2acbd72aea9441d90d3896fec053637-ASRReplica";
        final String filePath = "/subscriptions/758ad253-cbf5-4b18-8863-3eed0825bf07/resourceGroups/RG_RG/providers/Microsoft.Compute/disks/rg618_osdisk_1_d2acbd72aea9441d90d3896fec053637-ASRReplica";
        StorageConverter converter = new StorageConverter(SDKProbeType.AZURE);

        final EntityDTO.Builder storageEntityDto = createStorageEntityBuilder(storageId, doNotDelete, filePath);
        final EntityDTO.Builder storageTierEntity = createEntityBuilder(EntityType.STORAGE_TIER, storageTierId);
        final EntityDTO.Builder volumeEntity = createEntityBuilder(EntityType.VIRTUAL_VOLUME, volumeId);

        mockCloudDiscoveryConverterBehaviour(volumeId, filePath, storageEntityDto, storageTierEntity, volumeEntity);

        converter.convert(storageEntityDto, cloudDiscoveryConverter);

        assertNotNull(volumeEntity);
        assertNotNull(volumeEntity.getConsumerPolicy());
        assertThat(volumeEntity.getConsumerPolicy().getDeletable(), is(false));
    }

    /**
     * Test volume {@link com.vmturbo.platform.common.dto.CommonDTO.EntityDTO}'s AnalysisSettings.deletable
     * is true when its {@link com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ConsumerPolicy}.doNotDelete is false.
     */
    @Test
    public void testConvertWithConsumerPolicyDoNotDeleteFalse() {
        final boolean doNotDelete = false;
        final String volumeId = "::subscriptions::758ad253-cbf5-4b18-8863-3eed0825bf07::resourcegroups::rg_rg::providers::microsoft.compute::disks::rg618_lun_0_2_1c7102b8490b47fdabb5b6dc1891386f";
        final String filePath = "/subscriptions/758ad253-cbf5-4b18-8863-3eed0825bf07/resourceGroups/RG_RG/providers/Microsoft.Compute/disks/rg618_lun_0_2_1c7102b8490b47fdabb5b6dc1891386f";
        StorageConverter converter = new StorageConverter(SDKProbeType.AZURE);

        final EntityDTO.Builder storageEntityDto = createStorageEntityBuilder(storageId, doNotDelete, filePath);
        final EntityDTO.Builder storageTierEntity = createEntityBuilder(EntityType.STORAGE_TIER, storageTierId);
        final EntityDTO.Builder volumeEntity = createEntityBuilder(EntityType.VIRTUAL_VOLUME, volumeId);

        mockCloudDiscoveryConverterBehaviour(volumeId, filePath, storageEntityDto, storageTierEntity, volumeEntity);

        converter.convert(storageEntityDto, cloudDiscoveryConverter);

        assertNotNull(volumeEntity);
        assertNotNull(volumeEntity.getConsumerPolicy());
        assertThat(volumeEntity.getConsumerPolicy().getDeletable(), is(true));
    }

    /**
     * Helper method to create {@link EntityDTO.Builder}.
     *
     * @param entityType {@link EntityType}
     * @param entityId entity id
     * @return {@link EntityDTO.Builder}
     */
    private EntityDTO.Builder createEntityBuilder(@Nonnull final EntityType entityType,
                                                  @Nonnull final String entityId) {
        return EntityDTO.newBuilder()
            .setEntityType(entityType)
            .setId(entityId);
    }

    /**
     * Helper method to create storageEntity with one file in its storage data.
     *
     * @param storageId storage id
     * @param doNotDelete set its volume's StorageFileDescriptor to be doNotDelete
     * @param filePath its volume file path
     * @return {@link EntityDTO.Builder}
     */
    private EntityDTO.Builder createStorageEntityBuilder(@Nonnull final String storageId,
                                                         final boolean doNotDelete,
                                                         @Nonnull final String filePath) {
        return EntityDTO.newBuilder()
            .setId(storageId)
            .setEntityType(EntityType.STORAGE)
            .setStorageData(StorageData.newBuilder()
                .addFile(StorageFileDescriptor.newBuilder()
                    .setPath(filePath)
                    .setSizeKb(30L)
                    .setModificationTimeMs(1576180062673L)
                    .setDoNotDelete(doNotDelete)
                    .build())
                .build());
    }

    /**
     * Helper method to setup mock behaviour for cloudDiscoveryConverter.
     *
     * @param volumeId volume's id
     * @param filePath file path of volume
     * @param storageEntityDto  the storage {@link EntityDTO} the volume associated with
     * @param storageTierEntity storage tier entity which volume belongs to
     * @param volumeEntity the volume entity
     */
    private void mockCloudDiscoveryConverterBehaviour(@Nonnull final String volumeId,
                                                      @Nonnull final String filePath,
                                                      @Nonnull final EntityDTO.Builder storageEntityDto,
                                                      @Nonnull final EntityDTO.Builder storageTierEntity,
                                                      @Nonnull final EntityDTO.Builder volumeEntity) {
        when(cloudDiscoveryConverter.getStorageTier(eq(storageEntityDto))).thenReturn(storageTierId);
        when(cloudDiscoveryConverter.getStorageTierId(eq(storageTierId))).thenReturn(storageTierId);
        when(cloudDiscoveryConverter.getAvailabilityZone(eq(storageEntityDto))).thenReturn(Optional.of(azId));
        when(cloudDiscoveryConverter.getRegionIdFromAzId(eq(azId))).thenReturn(regionId);
        when(cloudDiscoveryConverter.getNewEntityBuilder(eq(storageTierId))).thenReturn(storageTierEntity);
        when(cloudDiscoveryConverter.getVolumeId(eq(regionName), eq(filePath))).thenReturn(Optional.of(volumeId));
        when(cloudDiscoveryConverter.getNewEntityBuilder(eq(volumeId))).thenReturn(volumeEntity);
    }
}
