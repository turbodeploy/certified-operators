package com.vmturbo.mediation.azure;

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
import com.vmturbo.mediation.cloud.util.TestUtils;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.SubDivisionData;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

public class AzureCloudDiscoveryConverterTest {

    private static final String AZURE_ENGINEERING_FILE_PATH =
            "src/test/resources/data/azure_engineering.management.core.windows.net.txt";

    private static final String businessAccountId = "758ad253-cbf5-4b18-8863-3eed0825bf07";

    private static CloudDiscoveryConverter azureConverter;

    private static CloudProviderConversionContext azureConversionContext;

    private static Map<EntityType, List<EntityDTO>> rawEntitiesByType;

    private static Map<EntityType, List<EntityDTO.Builder>> newEntitiesByType;

    @BeforeClass
    public static void setup() {
        DiscoveryResponse discoveryResponse = TestUtils.readResponseFromFile(AZURE_ENGINEERING_FILE_PATH);
        azureConversionContext = new AzureConversionContext();
        azureConverter = new CloudDiscoveryConverter(discoveryResponse, azureConversionContext);
        azureConverter.preProcess();

        rawEntitiesByType = discoveryResponse.getEntityDTOList().stream()
                .collect(Collectors.groupingBy(EntityDTO::getEntityType));
        newEntitiesByType = azureConverter.getNewEntitiesGroupedByType();
    }

    @Test
    public void testVirtualMachineConverter() {
        VirtualMachineConverter vmConverter = new VirtualMachineConverter(SDKProbeType.AZURE);
        EntityDTO.Builder ba = azureConverter.getNewEntityBuilder(businessAccountId);
        // get all original VMs and compare each with new VM
        rawEntitiesByType.get(EntityType.VIRTUAL_MACHINE).forEach(vm -> {
            String vmId = vm.getId();
            EntityDTO oldVM = azureConverter.getRawEntityDTO(vmId);
            EntityDTO.Builder newVM = azureConverter.getNewEntityBuilder(vmId);

            // check vm not removed
            assertTrue(vmConverter.convert(newVM, azureConverter));

            // some fields not modified
            verifyUnmodifiedFields(oldVM, newVM);

            // check VM buys LicenseAccess commodity from CT
            assertEquals(1, newVM.getCommoditiesBoughtList().stream()
                    .filter(commodityBought -> commodityBought.getProviderType() ==
                            EntityType.COMPUTE_TIER)
                    .flatMap(commodityBought -> commodityBought.getBoughtList().stream())
                    .filter(commodityDTO -> commodityDTO.getCommodityType() == CommodityType.LICENSE_ACCESS)
                    .count());

            // check 'active' for VM bought commodities not set to false
            newVM.getCommoditiesBoughtList().stream()
                    .flatMap(commodityBought -> commodityBought.getBoughtList().stream())
                    .filter(VirtualMachineConverter.COMMODITIES_TO_CLEAR_ACTIVE::contains)
                    .forEach(commodityDTO -> assertTrue(commodityDTO.getActive()));

            // check providers changed (vm may consumes multiple storages, convert each to new type)
            verifyProvidersChanged(oldVM, newVM, ImmutableMap.of(
                    EntityType.STORAGE, EntityType.STORAGE_TIER,
                    EntityType.PHYSICAL_MACHINE, EntityType.COMPUTE_TIER
            ));

            // check old connected to
            assertEquals(0, oldVM.getLayeredOverCount());

            // check new connected to
            Map<EntityType, List<EntityDTO.Builder>> layeredOver = newVM.getLayeredOverList().stream()
                    .map(id -> azureConverter.getNewEntityBuilder(id))
                    .collect(Collectors.toMap(EntityDTO.Builder::getEntityType,
                            Lists::newArrayList, (a, b) -> {
                                a.addAll(b);
                                return a;
                            }));
            // connected to region
            assertTrue(layeredOver.containsKey(EntityType.REGION));
            assertEquals(1, layeredOver.get(EntityType.REGION).size());
            // may or may not be connected to volume
            Map<String, SubDivisionData> oldSubDivisionById = oldVM.getCommoditiesBoughtList().stream()
                    .filter(CommodityBought::hasSubDivision)
                    .map(CommodityBought::getSubDivision)
                    .collect(Collectors.toMap(SubDivisionData::getSubDivisionId, Function.identity()));

            if (oldSubDivisionById.isEmpty()) {
                // not connected to volume
                assertNull(layeredOver.get(EntityType.VIRTUAL_VOLUME));
            } else {
                // connected to volume
                assertThat(layeredOver.get(EntityType.VIRTUAL_VOLUME).stream()
                                .map(EntityDTO.Builder::getId).collect(Collectors.toList()),
                        containsInAnyOrder(oldSubDivisionById.keySet().toArray()));

                // check volumes
                layeredOver.get(EntityType.VIRTUAL_VOLUME).forEach(volume -> {
                    SubDivisionData subDivisionData = oldSubDivisionById.get(volume.getId());
                    // check volume properties
                    assertEquals(subDivisionData.getDisplayName(), volume.getDisplayName());
                    assertEquals(subDivisionData.getStorageAccessCapacity(),
                            volume.getVirtualVolumeData().getStorageAccessCapacity(), 0);
                    assertEquals(subDivisionData.getStorageAmountCapacity(),
                            volume.getVirtualVolumeData().getStorageAmountCapacity(), 0);

                    // volumes are connected to AZ and StorageTier
                    assertThat(volume.getLayeredOverList().stream()
                            .map(id -> azureConverter.getNewEntityBuilder(id).getEntityType())
                            .collect(Collectors.toList()), containsInAnyOrder(
                            EntityType.REGION, EntityType.STORAGE_TIER));

                    // volume ownedby BusinsesAccount
                    assertThat(ba.getConsistsOfList(), hasItem(volume.getId()));
                });
            }

            // check vm owned by BusinessAccount
            assertThat(ba.getConsistsOfList(), hasItem(vmId));
        });
    }

    @Test
    public void testDatabaseServerConverter() {
        DatabaseServerConverter dbsConverter = new DatabaseServerConverter(SDKProbeType.AZURE);
        // get all original VMs and compare each with new VM
        rawEntitiesByType.get(EntityType.DATABASE_SERVER).forEach(dbs -> {
            EntityDTO.Builder newDBS = azureConverter.getNewEntityBuilder(dbs.getId());
            // check dbs removed
            assertFalse(dbsConverter.convert(newDBS, azureConverter));
        });
    }

    @Test
    public void testDatabaseConverter() {
        IEntityConverter converter = new DatabaseConverter(SDKProbeType.AZURE);
        rawEntitiesByType.get(EntityType.DATABASE).forEach(entity -> {
            String dbId = entity.getId();
            EntityDTO oldEntity = azureConverter.getRawEntityDTO(dbId);
            EntityDTO.Builder newEntity = azureConverter.getNewEntityBuilder(dbId);

            // check db not removed
            assertTrue(converter.convert(newEntity, azureConverter));

            // check unmodified fields
            verifyUnmodifiedFields(oldEntity, newEntity);

            // connected to Region
            assertEquals(0, oldEntity.getLayeredOverCount());
            assertEquals(1, newEntity.getLayeredOverCount());
            assertEquals(EntityType.REGION, azureConverter.getNewEntityBuilder(
                    newEntity.getLayeredOver(0)).getEntityType());

            // check db owned by BusinessAccount
            assertThat(azureConverter.getNewEntityBuilder(businessAccountId).getConsistsOfList(), hasItem(dbId));
        });
    }

    @Test
    public void testComputeTierConverter() {
        IEntityConverter converter = new ComputeTierConverter(SDKProbeType.AZURE);
        newEntitiesByType.get(EntityType.COMPUTE_TIER).forEach(entity -> {
            String entityId = entity.getId();
            EntityDTO.Builder newEntity = azureConverter.getNewEntityBuilder(entityId);

            // check ct not removed
            assertTrue(converter.convert(newEntity, azureConverter));

            // connected to storage tier and region
            Set<EntityType> connectedEntityTypes = newEntity.getLayeredOverList().stream()
                    .map(id -> azureConverter.getNewEntityBuilder(id).getEntityType())
                    .collect(Collectors.toSet());
            assertThat(connectedEntityTypes, containsInAnyOrder(EntityType.REGION, EntityType.STORAGE_TIER));

            // check bought commodities
            assertEquals(0, newEntity.getCommoditiesBoughtCount());

            // check sold commodities
            assertThat(newEntity.getCommoditiesSoldList().stream()
                    .map(CommodityDTO::getCommodityType)
                    .collect(Collectors.toSet()), containsInAnyOrder(CommodityType.CPU,
                    CommodityType.CPU_PROVISIONED, CommodityType.MEM, CommodityType.MEM_PROVISIONED,
                    CommodityType.IO_THROUGHPUT, CommodityType.NET_THROUGHPUT,
                    CommodityType.NUM_DISK, CommodityType.LICENSE_ACCESS));

            // check ct owned by CloudService
            assertThat(azureConverter.getNewEntityBuilder(
                    CloudService.AZURE_VIRTUAL_MACHINES.getId()).getConsistsOfList(), hasItem(entityId));
        });
    }

    @Test
    public void testDatabaseTierConverter() {
        IEntityConverter converter = new DatabaseTierConverter();
        newEntitiesByType.get(EntityType.DATABASE_TIER).forEach(entity -> {
            String entityId = entity.getId();
            EntityDTO.Builder newEntity = azureConverter.getNewEntityBuilder(entityId);

            // check dt not removed
            assertTrue(converter.convert(newEntity, azureConverter));

            // connected to region
            Set<EntityType> connectedEntityTypes = newEntity.getLayeredOverList().stream()
                    .map(id -> azureConverter.getNewEntityBuilder(id).getEntityType())
                    .collect(Collectors.toSet());
            assertThat(connectedEntityTypes, containsInAnyOrder(EntityType.REGION));

            // check bought commodities
            assertEquals(0, newEntity.getCommoditiesBoughtCount());

            // check sold commodities
            assertThat(newEntity.getCommoditiesSoldList().stream()
                    .map(CommodityDTO::getCommodityType)
                    .collect(Collectors.toSet()), containsInAnyOrder(CommodityType.DB_MEM,
                    CommodityType.TRANSACTION, CommodityType.TRANSACTION_LOG,
                    CommodityType.CONNECTION, CommodityType.DB_CACHE_HIT_RATE,
                    CommodityType.RESPONSE_TIME, CommodityType.APPLICATION, CommodityType.LICENSE_ACCESS));

            // check that database tier is owned by cloud service
            assertThat(azureConverter.getNewEntityBuilder(CloudService.AZURE_DATA_SERVICES.getId())
                            .getConsistsOfList(), hasItem(entityId));
        });
    }

    @Test
    public void testStorageTierConverter() {
        IEntityConverter converter = new StorageConverter(SDKProbeType.AZURE);
        rawEntitiesByType.get(EntityType.STORAGE).forEach(entity -> {

            String storageTierId = azureConverter.getStorageTierId(entity.getStorageData().getStorageTier());

            EntityDTO.Builder storage = azureConverter.getNewEntityBuilder(entity.getId());
            EntityDTO.Builder storageTier = azureConverter.getNewEntityBuilder(storageTierId);

            // check storage IS removed
            assertFalse(converter.convert(storage, azureConverter));

            // connected to region
            Set<EntityType> connectedEntityTypes = storageTier.getLayeredOverList().stream()
                    .map(id -> azureConverter.getNewEntityBuilder(id).getEntityType())
                    .collect(Collectors.toSet());
            assertThat(connectedEntityTypes, containsInAnyOrder(EntityType.REGION));

            // check bought commodities
            assertEquals(0, storageTier.getCommoditiesBoughtCount());

            // check sold commodities
            assertThat(storageTier.getCommoditiesSoldList().stream()
                            .map(CommodityDTO::getCommodityType)
                            .collect(Collectors.toSet()), containsInAnyOrder(
                    CommodityType.STORAGE_ACCESS, CommodityType.STORAGE_AMOUNT,
                    CommodityType.STORAGE_CLUSTER, CommodityType.STORAGE_LATENCY,
                    CommodityType.STORAGE_PROVISIONED));

            // check storage tier owned by CloudService
            assertThat(azureConverter.getNewEntityBuilder(CloudService.AZURE_STORAGE.getId())
                            .getConsistsOfList(), hasItem(storageTierId));

            // check volumes
            entity.getCommoditiesSoldList().stream()
                    .filter(commodity -> commodity.getCommodityType() == CommodityType.DSPM_ACCESS)
                    .map(commodityDTO -> CloudDiscoveryConverter.getRegionNameFromAzId(
                            CloudDiscoveryConverter.keyToUuid(commodityDTO.getKey())))
                    .findAny()
                    .ifPresent(regionId ->
                        entity.getStorageData().getFileList()
                                .forEach(file -> azureConverter.getVolumeId("", file.getPath()).ifPresent(volumeId -> {
                                    EntityDTO.Builder volume = azureConverter.getNewEntityBuilder(volumeId);

                                    // check volume properties
                                    assertTrue(file.getPath().contains(volume.getDisplayName()));
                                    assertEquals(file.getSizeKb() / 1024,
                                            volume.getVirtualVolumeData().getStorageAmountCapacity(), 0);
                                    assertTrue(file.hasRedundancyType());
                                    assertTrue(volume.getVirtualVolumeData().hasRedundancyType());
                                    assertEquals(file.getRedundancyType(),
                                            volume.getVirtualVolumeData().getRedundancyType().toString());

                                    // check volumes are connected to region and storage tier
                                    assertThat(volume.getLayeredOverList()
                                            .stream()
                                            .map(id -> azureConverter.getNewEntityBuilder(id).getEntityType())
                                            .collect(Collectors.toList()), containsInAnyOrder(
                                                    EntityType.REGION, EntityType.STORAGE_TIER));

                                    // volume ownedby BusinsesAccount
                                    assertThat(azureConverter.getNewEntityBuilder(businessAccountId)
                                                    .getConsistsOfList(), hasItem(volume.getId()));
                                }))
                    );

        });

        // check all storage tiers are connected to 30 regions after converting all storages
        azureConverter.getAllStorageTierIds().forEach(s ->
            assertEquals(30, azureConverter.getNewEntityBuilder(s).getLayeredOverCount()));
    }

    @Test
    public void testAvailabilityZoneConverter() {
        IEntityConverter converter = new AvailabilityZoneConverter(SDKProbeType.AZURE);
        newEntitiesByType.get(EntityType.AVAILABILITY_ZONE).forEach(entity -> {
            EntityDTO.Builder newEntity = azureConverter.getNewEntityBuilder(entity.getId());
            // check az IS removed
            assertFalse(converter.convert(newEntity, azureConverter));
        });
    }

    @Test
    public void testRegionConverter() {
        IEntityConverter converter = new RegionConverter();
        newEntitiesByType.get(EntityType.REGION).forEach(entity -> {
            String entityId = entity.getId();
            EntityDTO.Builder newEntity = azureConverter.getNewEntityBuilder(entityId);

            // check region not removed
            assertTrue(converter.convert(newEntity, azureConverter));

            // check no bought commodities
            assertEquals(0, newEntity.getCommoditiesBoughtCount());
            // check only one sold commodities: DataCenter
            assertEquals(1, newEntity.getCommoditiesSoldCount());
            assertEquals(CommodityType.DATACENTER, newEntity.getCommoditiesSold(0).getCommodityType());
            assertEquals("DataCenter::" + entityId, newEntity.getCommoditiesSold(0).getKey());
        });
    }

    @Test
    public void testBusinessAccountConverter() {
        IEntityConverter converter = new BusinessAccountConverter(SDKProbeType.AZURE);
        rawEntitiesByType.get(EntityType.BUSINESS_ACCOUNT).forEach(entity -> {
            String dbId = entity.getId();
            EntityDTO oldEntity = azureConverter.getRawEntityDTO(dbId);
            EntityDTO.Builder newEntity = azureConverter.getNewEntityBuilder(dbId);

            // check ba not removed
            assertTrue(converter.convert(newEntity, azureConverter));

            // check unmodified fields
            assertEquals(oldEntity.getDisplayName(), newEntity.getDisplayName());
            assertEquals(oldEntity.getBusinessAccountData(), newEntity.getBusinessAccountData());
        });
    }

    @Test
    public void testApplicationConverter() {
        IEntityConverter converter = new VirtualApplicationConverter();
        rawEntitiesByType.get(EntityType.APPLICATION).forEach(entity -> {
            String entityId = entity.getId();
            EntityDTO oldEntity = azureConverter.getRawEntityDTO(entityId);
            EntityDTO.Builder newEntity = azureConverter.getNewEntityBuilder(entityId);

            assertTrue(converter.convert(newEntity, azureConverter));
            assertEquals(oldEntity, newEntity.build());
        });
    }

    @Test
    public void testDiskArrayConverter() {
        IEntityConverter converter = new DiskArrayConverter();
        newEntitiesByType.get(EntityType.DISK_ARRAY).forEach(entity ->
                assertFalse(converter.convert(entity, azureConverter)));
    }

    /**
     * Verify that the commodity providers are changed to new types.
     */
    private static void verifyProvidersChanged(EntityDTO oldEntity, EntityDTO.Builder newEntity,
            Map<EntityType, EntityType> oldToNewProviderTypeMapping) {
        Object[] newProviderTypes = TestUtils.getOldProviderTypes(oldEntity, azureConverter).stream()
                .map(entityType -> oldToNewProviderTypeMapping.getOrDefault(entityType, entityType))
                .toArray();
        assertThat(TestUtils.getNewProviderTypes(newEntity, azureConverter), containsInAnyOrder(newProviderTypes));
    }

    /**
     * Verify that some fields in EntityDTO are not modified.
     */
    private void verifyUnmodifiedFields(EntityDTO oldEntity, EntityDTO.Builder newEntity) {
        assertEquals(oldEntity.getDisplayName(), newEntity.getDisplayName());
        assertEquals(oldEntity.getCommoditiesSoldList(), newEntity.getCommoditiesSoldList());
        assertEquals(oldEntity.getProfileId(), newEntity.getProfileId());
        assertEquals(oldEntity.getEntityPropertiesList(), newEntity.getEntityPropertiesList());
        assertEquals(oldEntity.getPowerState(), newEntity.getPowerState());
        assertEquals(oldEntity.getConsumerPolicy(), newEntity.getConsumerPolicy());

        if (oldEntity.getEntityType() == EntityType.VIRTUAL_MACHINE) {
            assertEquals(oldEntity.getVirtualMachineData(), newEntity.getVirtualMachineData());
        } else if (oldEntity.getEntityType() == EntityType.DATABASE) {
            assertEquals(oldEntity.getApplicationData(), newEntity.getApplicationData());
        }
    }
}
