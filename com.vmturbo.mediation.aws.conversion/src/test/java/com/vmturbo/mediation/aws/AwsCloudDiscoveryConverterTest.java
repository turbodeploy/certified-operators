package com.vmturbo.mediation.aws;

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
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.mediation.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.cloud.CloudProviderConversionContext;
import com.vmturbo.mediation.cloud.IEntityConverter;
import com.vmturbo.mediation.cloud.converter.ApplicationConverter;
import com.vmturbo.mediation.cloud.converter.AvailabilityZoneConverter;
import com.vmturbo.mediation.cloud.converter.BusinessAccountConverter;
import com.vmturbo.mediation.cloud.converter.ComputeTierConverter;
import com.vmturbo.mediation.cloud.converter.DatabaseConverter;
import com.vmturbo.mediation.cloud.converter.DatabaseServerConverter;
import com.vmturbo.mediation.cloud.converter.DatabaseServerTierConverter;
import com.vmturbo.mediation.cloud.converter.DefaultConverter;
import com.vmturbo.mediation.cloud.converter.DiskArrayConverter;
import com.vmturbo.mediation.cloud.converter.LoadBalancerConverter;
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

public class AwsCloudDiscoveryConverterTest {

    private static final String AWS_ENGINEERING_FILE_PATH =
            "src/test/resources/data/aws_engineering.aws.amazon.com.txt";

    private static final String masterAccountId = "192821421245";

    private static CloudDiscoveryConverter awsConverter;

    private static CloudProviderConversionContext awsConversionContext;

    private static Map<EntityType, List<EntityDTO>> rawEntitiesByType;

    private static Map<EntityType, List<EntityDTO.Builder>> newEntitiesByType;

    @BeforeClass
    public static void setup() {
        DiscoveryResponse discoveryResponse = TestUtils.readResponseFromFile(AWS_ENGINEERING_FILE_PATH);
        awsConversionContext = new AwsConversionContext();
        awsConverter = new CloudDiscoveryConverter(discoveryResponse, awsConversionContext);
        awsConverter.preProcess();

        rawEntitiesByType = discoveryResponse.getEntityDTOList().stream()
                .collect(Collectors.groupingBy(EntityDTO::getEntityType));
        newEntitiesByType = awsConverter.getNewEntitiesGroupedByType();
    }

    @Test
    public void testVirtualMachineConverter() {
        VirtualMachineConverter vmConverter = new VirtualMachineConverter(SDKProbeType.AWS);
        EntityDTO.Builder ba = awsConverter.getNewEntityBuilder(masterAccountId);
        // get all original VMs and compare each with new VM
        rawEntitiesByType.get(EntityType.VIRTUAL_MACHINE).forEach(vm -> {
            String vmId = vm.getId();
            EntityDTO oldVM = awsConverter.getRawEntityDTO(vmId);
            EntityDTO.Builder newVM = awsConverter.getNewEntityBuilder(vmId);

            // check fake vm (without profileId) removed
            if (!vm.hasProfileId()) {
                assertFalse(vmConverter.convert(newVM, awsConverter));
                return;
            }

            // check real vm not removed
            assertTrue(vmConverter.convert(newVM, awsConverter));

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

            // check old connected
            assertEquals(0, oldVM.getLayeredOverCount());

            // check new connected
            Map<EntityType, List<EntityDTO.Builder>> layeredOver = newVM.getLayeredOverList().stream()
                    .map(id -> awsConverter.getNewEntityBuilder(id))
                    .collect(Collectors.toMap(EntityDTO.Builder::getEntityType,
                            k -> Lists.newArrayList(k), (a, b) -> {
                                a.addAll(b);
                                return a;
                            }));
            // connected to AZ
            assertTrue(layeredOver.containsKey(EntityType.AVAILABILITY_ZONE));
            assertEquals(1, layeredOver.get(EntityType.AVAILABILITY_ZONE).size());
            // may or may not be connected to volume
            Map<String, SubDivisionData> oldSubDivisionById = oldVM.getCommoditiesBoughtList().stream()
                    .filter(CommodityBought::hasSubDivision)
                    .map(CommodityBought::getSubDivision)
                    .collect(Collectors.toMap(SubDivisionData::getSubDivisionId, k -> k));

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
                                    .map(id -> awsConverter.getNewEntityBuilder(id).getEntityType())
                                    .collect(Collectors.toList()), containsInAnyOrder(
                                            EntityType.AVAILABILITY_ZONE, EntityType.STORAGE_TIER));

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
        DatabaseServerConverter dbsConverter = new DatabaseServerConverter(SDKProbeType.AWS);
        // get all original VMs and compare each with new VM
        rawEntitiesByType.get(EntityType.DATABASE_SERVER).forEach(dbs -> {
            String dbsId = dbs.getId();
            EntityDTO oldDBS = awsConverter.getRawEntityDTO(dbsId);
            EntityDTO.Builder newDBS = awsConverter.getNewEntityBuilder(dbsId);

            // check dbs not removed
            assertTrue(dbsConverter.convert(newDBS, awsConverter));

            // fields not modified
            verifyUnmodifiedFields(oldDBS, newDBS);

            // check providers changed
            verifyProvidersChanged(oldDBS, newDBS, ImmutableMap.of(
                    EntityType.VIRTUAL_MACHINE, EntityType.DATABASE_SERVER_TIER));

            // connected to AZ
            assertEquals(0, oldDBS.getLayeredOverCount());
            assertEquals(1, newDBS.getLayeredOverCount());
            assertEquals(EntityType.AVAILABILITY_ZONE, awsConverter.getNewEntityBuilder(
                    newDBS.getLayeredOver(0)).getEntityType());

            // check dbs owned by BusinessAccount
            assertThat(awsConverter.getNewEntityBuilder(masterAccountId).getConsistsOfList(), hasItem(dbsId));
        });
    }

    @Test
    public void testDatabaseConverter() {
        IEntityConverter converter = new DatabaseConverter(SDKProbeType.AWS);
        rawEntitiesByType.get(EntityType.DATABASE).forEach(entity -> {
            String dbId = entity.getId();
            EntityDTO oldEntity = awsConverter.getRawEntityDTO(dbId);
            EntityDTO.Builder newEntity = awsConverter.getNewEntityBuilder(dbId);

            // check db not removed
            assertTrue(converter.convert(newEntity, awsConverter));

            // check unmodified fields
            verifyUnmodifiedFields(oldEntity, newEntity);

            // connected to AZ
            assertEquals(0, oldEntity.getLayeredOverCount());
            assertEquals(1, newEntity.getLayeredOverCount());
            assertEquals(EntityType.AVAILABILITY_ZONE, awsConverter.getNewEntityBuilder(
                    newEntity.getLayeredOver(0)).getEntityType());

            // check db owned by BusinessAccount
            assertThat(awsConverter.getNewEntityBuilder(masterAccountId).getConsistsOfList(), hasItem(dbId));
        });
    }

    @Test
    public void testComputeTierConverter() {
        IEntityConverter converter = new ComputeTierConverter(SDKProbeType.AWS);
        newEntitiesByType.get(EntityType.COMPUTE_TIER).forEach(entity -> {
            String entityId = entity.getId();
            EntityDTO.Builder newEntity = awsConverter.getNewEntityBuilder(entityId);

            // check ct not removed
            assertTrue(converter.convert(newEntity, awsConverter));

            // connected to storage tier and region
            Set<EntityType> connectedEntityTypes = newEntity.getLayeredOverList().stream()
                    .map(id -> awsConverter.getNewEntityBuilder(id).getEntityType())
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
            assertThat(awsConverter.getNewEntityBuilder(CloudService.AWS_EC2.getId()).getConsistsOfList(),
                    hasItem(entityId));
        });
    }

    @Test
    public void testDatabaseServerTierConverter() {
        IEntityConverter converter = new DatabaseServerTierConverter();
        newEntitiesByType.get(EntityType.DATABASE_SERVER_TIER).forEach(entity -> {
            String entityId = entity.getId();
            EntityDTO.Builder newEntity = awsConverter.getNewEntityBuilder(entityId);

            // check dt not removed
            assertTrue(converter.convert(newEntity, awsConverter));

            // connected to region
            Set<EntityType> connectedEntityTypes = newEntity.getLayeredOverList().stream()
                    .map(id -> awsConverter.getNewEntityBuilder(id).getEntityType())
                    .collect(Collectors.toSet());
            assertThat(connectedEntityTypes, containsInAnyOrder(EntityType.REGION));

            // check bought commodities
            assertEquals(0, newEntity.getCommoditiesBoughtCount());

            // check sold commodities
            assertThat(newEntity.getCommoditiesSoldList().stream()
                    .map(CommodityDTO::getCommodityType)
                    .collect(Collectors.toSet()), containsInAnyOrder(CommodityType.VMEM,
                    CommodityType.VCPU, CommodityType.VSTORAGE, CommodityType.IO_THROUGHPUT,
                    CommodityType.APPLICATION, CommodityType.LICENSE_ACCESS));

            // check that database server tier is owned by cloud service
            assertThat(awsConverter.getNewEntityBuilder(
                    CloudService.AWS_RDS.getId()).getConsistsOfList(), hasItem(entityId));
        });
    }

    @Test
    public void testStorageTierConverter() {
        IEntityConverter converter = new StorageConverter(SDKProbeType.AWS);
        rawEntitiesByType.get(EntityType.STORAGE).forEach(entity -> {

            String storageTierId = awsConversionContext.getStorageTierId(entity.getStorageData().getStorageTier());

            EntityDTO.Builder storage = awsConverter.getNewEntityBuilder(entity.getId());
            EntityDTO.Builder storageTier = awsConverter.getNewEntityBuilder(storageTierId);

            // check storage IS removed
            assertFalse(converter.convert(storage, awsConverter));

            // connected to region
            Set<EntityType> connectedEntityTypes = storageTier.getLayeredOverList().stream()
                    .map(id -> awsConverter.getNewEntityBuilder(id).getEntityType())
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
            assertThat(awsConverter.getNewEntityBuilder(CloudService.AWS_EBS.getId())
                    .getConsistsOfList(), hasItem(storageTierId));

            // check volumes
            entity.getCommoditiesSoldList().stream()
                    .filter(commodity -> commodity.getCommodityType() == CommodityType.DSPM_ACCESS)
                    .map(commodityDTO -> CloudDiscoveryConverter.keyToUuid(commodityDTO.getKey()))
                    .findAny()
                    .ifPresent(azId -> {
                        String regionName = CloudDiscoveryConverter.getRegionNameFromAzId(azId);
                        entity.getStorageData().getFileList().forEach(file ->
                            awsConverter.getVolumeId(regionName, file.getPath()).ifPresent(volumeId -> {
                                EntityDTO.Builder volume = awsConverter.getNewEntityBuilder(volumeId);

                                // check volume properties
                                assertEquals(file.getPath(), volume.getDisplayName());
                                assertEquals(file.getIopsProvisioned(),
                                        volume.getVirtualVolumeData().getStorageAccessCapacity(), 0);
                                assertEquals(file.getSizeKb() / 1024,
                                        volume.getVirtualVolumeData().getStorageAmountCapacity(), 0);
                                assertFalse(file.hasRedundancyType());
                                assertFalse(volume.getVirtualVolumeData().hasRedundancyType());

                                // volumes are connected to AZ and StorageTier
                                assertThat(volume.getLayeredOverList().stream()
                                        .map(id -> awsConverter.getNewEntityBuilder(id).getEntityType())
                                        .collect(Collectors.toList()), containsInAnyOrder(
                                        EntityType.AVAILABILITY_ZONE, EntityType.STORAGE_TIER));

                                // volume ownedby BusinsesAccount
                                assertThat(awsConverter.getNewEntityBuilder(masterAccountId)
                                        .getConsistsOfList(), hasItem(volume.getId()));
                            })
                        );
                    });
        });

        // check all storage tiers are connected to 15 regions after converting all storages
        awsConverter.getAllStorageTierIds().forEach(s ->
            assertEquals(15, awsConverter.getNewEntityBuilder(s).getLayeredOverCount()));
    }

    @Test
    public void testAvailabilityZoneConverter() {
        IEntityConverter converter = new AvailabilityZoneConverter(SDKProbeType.AWS);
        newEntitiesByType.get(EntityType.AVAILABILITY_ZONE).forEach(entity -> {
            String entityId = entity.getId();
            EntityDTO.Builder newEntity = awsConverter.getNewEntityBuilder(entityId);

            // check az not removed
            assertTrue(converter.convert(newEntity, awsConverter));

            // check no commodities
            assertEquals(0, newEntity.getCommoditiesBoughtCount());
            assertEquals(0, newEntity.getCommoditiesSoldCount());

            // check no connected to
            assertEquals(0, newEntity.getLayeredOverCount());

            // check region owns AZ
            assertEquals(Sets.newHashSet(EntityType.AVAILABILITY_ZONE),
                    awsConverter.getNewEntityBuilder(awsConversionContext.getRegionIdFromAzId(entityId))
                            .getConsistsOfList().stream()
                            .map(id -> awsConverter.getNewEntityBuilder(id).getEntityType())
                            .collect(Collectors.toSet()));
        });
    }

    @Test
    public void testRegionConverter() {
        IEntityConverter converter = new RegionConverter();
        newEntitiesByType.get(EntityType.REGION).forEach(entity -> {
            String entityId = entity.getId();
            EntityDTO.Builder newEntity = awsConverter.getNewEntityBuilder(entityId);

            // check region not removed
            assertTrue(converter.convert(newEntity, awsConverter));

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
        IEntityConverter converter = new BusinessAccountConverter(SDKProbeType.AWS);
        rawEntitiesByType.get(EntityType.BUSINESS_ACCOUNT).forEach(entity -> {
            String dbId = entity.getId();
            EntityDTO oldEntity = awsConverter.getRawEntityDTO(dbId);
            EntityDTO.Builder newEntity = awsConverter.getNewEntityBuilder(dbId);

            // check ba not removed
            assertTrue(converter.convert(newEntity, awsConverter));

            // check unmodified fields
            assertEquals(oldEntity.getDisplayName(), newEntity.getDisplayName());
            assertEquals(oldEntity.getBusinessAccountData(), newEntity.getBusinessAccountData());

            // master account owns sub account
            if (newEntity.getId().equals(masterAccountId)) {
                assertThat(newEntity.getConsistsOfList(), containsInAnyOrder("323871187550",
                        "001844731978"));
            }
        });
    }

    @Test
    public void testLoadBalancerConverter() {
        IEntityConverter converter = new LoadBalancerConverter();
        rawEntitiesByType.get(EntityType.LOAD_BALANCER).forEach(entity ->
                convertAndVerifyEntityUnmodified(converter, entity.getId()));
    }

    @Test
    public void testVirtualApplicationConverter() {
        IEntityConverter converter = new ApplicationConverter();
        rawEntitiesByType.get(EntityType.VIRTUAL_APPLICATION).forEach(entity ->
                convertAndVerifyEntityUnmodified(converter, entity.getId()));
    }

    @Test
    public void testApplicationConverter() {
        IEntityConverter converter = new VirtualApplicationConverter();
        rawEntitiesByType.get(EntityType.APPLICATION).forEach(entity ->
                convertAndVerifyEntityUnmodified(converter, entity.getId()));
    }

    @Test
    public void testReservedInstanceConverter() {
        IEntityConverter converter = new DefaultConverter();
        rawEntitiesByType.get(EntityType.RESERVED_INSTANCE).forEach(entity ->
                convertAndVerifyEntityUnmodified(converter, entity.getId()));
    }

    @Test
    public void testDiskArrayConverter() {
        IEntityConverter converter = new DiskArrayConverter();
        newEntitiesByType.get(EntityType.DISK_ARRAY).forEach(entity ->
                assertFalse(converter.convert(entity, awsConverter)));
    }

    /**
     * Convert the entity and verify that it is the same after convert.
     */
    private void convertAndVerifyEntityUnmodified(IEntityConverter converter, String entityId) {
        EntityDTO oldEntity = awsConverter.getRawEntityDTO(entityId);
        EntityDTO.Builder newEntity = awsConverter.getNewEntityBuilder(entityId);
        boolean keep = converter.convert(newEntity, awsConverter);

        assertTrue(keep);
        assertEquals(oldEntity, newEntity.build());

        if (oldEntity.getEntityType() != EntityType.RESERVED_INSTANCE) {
            assertThat(awsConverter.getNewEntityBuilder(masterAccountId).getConsistsOfList(), hasItem(entityId));
        }
    }

    /**
     * Verify that the commodity providers are changed to new types.
     */
    private static void verifyProvidersChanged(EntityDTO oldEntity, EntityDTO.Builder newEntity,
            Map<EntityType, EntityType> oldToNewProviderTypeMapping) {
        Object[] newProviderTypes = TestUtils.getOldProviderTypes(oldEntity, awsConverter).stream()
                .map(entityType -> oldToNewProviderTypeMapping.getOrDefault(entityType, entityType))
                .toArray();
        assertThat(TestUtils.getNewProviderTypes(newEntity, awsConverter), containsInAnyOrder(newProviderTypes));
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
