package com.vmturbo.mediation.azure.volumes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.vmturbo.mediation.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.cloud.CloudProviderConversionContext;
import com.vmturbo.mediation.cloud.IEntityConverter;
import com.vmturbo.mediation.cloud.converter.AvailabilityZoneConverter;
import com.vmturbo.mediation.cloud.converter.BusinessAccountConverter;
import com.vmturbo.mediation.cloud.converter.ComputeTierConverter;
import com.vmturbo.mediation.cloud.converter.DatabaseConverter;
import com.vmturbo.mediation.cloud.converter.DatabaseServerConverter;
import com.vmturbo.mediation.cloud.converter.DatabaseTierConverter;
import com.vmturbo.mediation.cloud.converter.DiskArrayConverter;
import com.vmturbo.mediation.cloud.converter.RegionConverter;
import com.vmturbo.mediation.cloud.converter.StorageConverter;
import com.vmturbo.mediation.cloud.converter.VirtualApplicationConverter;
import com.vmturbo.mediation.cloud.converter.VirtualMachineConverter;
import com.vmturbo.mediation.cloud.util.CloudService;
import com.vmturbo.mediation.cloud.util.ConverterUtils;
import com.vmturbo.mediation.cloud.util.TestUtils;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.SubDivisionData;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

public class AzureVolumesCloudDiscoveryConverterTest {

    private static final String AZURE_ENGINEERING_FILE_PATH =
            "src/test/resources/data/azure_wasted_volumes_engineering.management.core.windows.net.txt";

    private static AzureVolumesCloudDiscoveryConverter azureVolumesConverter;

    private static AzureVolumesConversionContext azureVolumesConversionContext;

    private static Map<EntityType, List<EntityDTO>> rawEntitiesByType;

    private static Map<EntityType, List<EntityDTO.Builder>> newEntitiesByType;

    @BeforeClass
    public static void setup() {
        DiscoveryResponse discoveryResponse = TestUtils.readResponseFromFile(AZURE_ENGINEERING_FILE_PATH);
        azureVolumesConversionContext = new AzureVolumesConversionContext();
        azureVolumesConverter = new AzureVolumesCloudDiscoveryConverter(discoveryResponse,
            azureVolumesConversionContext);
        azureVolumesConverter.preProcess();

        rawEntitiesByType = discoveryResponse.getEntityDTOList().stream()
                .collect(Collectors.groupingBy(EntityDTO::getEntityType));
        newEntitiesByType = azureVolumesConverter.getNewEntitiesGroupedByType();
    }

    @Test
    public void testStorageTierConverter() {
        IEntityConverter converter = new StorageConverter(SDKProbeType.AZURE_STORAGE_BROWSE);
        rawEntitiesByType.get(EntityType.STORAGE).forEach(entity -> {

            String storageTierId = azureVolumesConverter
                .getStorageTierId(azureVolumesConverter.getStorageTier(entity.toBuilder()));

            EntityDTO.Builder storage = azureVolumesConverter.getNewEntityBuilder(entity.getId());
            EntityDTO.Builder storageTier = azureVolumesConverter.getNewEntityBuilder(storageTierId);

            // check storage IS removed
            assertFalse(converter.convert(storage, azureVolumesConverter));

            // check volumes
            azureVolumesConversionContext.getAvailabilityZone(entity.toBuilder())
                .ifPresent(azId ->
                    entity.getStorageData().getFileList()
                        .forEach(file -> azureVolumesConverter
                            .getVolumeId("", file.getPath())
                            .ifPresent(volumeId -> {
                                EntityDTO.Builder volume =
                                    azureVolumesConverter.getNewEntityBuilder(volumeId);

                                // check volume properties
                                assertTrue(file.getPath().contains(volume.getDisplayName()));
                                assertEquals(file.getSizeKb() / 1024.0f,
                                    volume.getVirtualVolumeData().getStorageAmountCapacity(),
                                    0.01);
                                assertTrue(file.hasRedundancyType());
                                assertTrue(volume.getVirtualVolumeData().hasRedundancyType());
                                assertEquals(file.getRedundancyType(),
                                    volume.getVirtualVolumeData().getRedundancyType()
                                        .toString());

                                // check volumes are connected to region and storage tier
                                assertThat(volume.getLayeredOverList()
                                    .stream()
                                    .map(id -> azureVolumesConverter.getNewEntityBuilder(id).getEntityType())
                                    .collect(Collectors.toList()), containsInAnyOrder(
                                    EntityType.STORAGE_TIER, EntityType.REGION));
                            }))
                );

        });
    }
}
