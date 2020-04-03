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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Lists;

import org.junit.BeforeClass;
import org.junit.Test;

import com.vmturbo.mediation.conversion.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.conversion.cloud.CloudProviderConversionContext;
import com.vmturbo.mediation.conversion.cloud.IEntityConverter;
import com.vmturbo.mediation.conversion.cloud.converter.BusinessAccountConverter;
import com.vmturbo.mediation.conversion.cloud.converter.DefaultConverter;
import com.vmturbo.mediation.conversion.cloud.converter.VirtualMachineConverter;
import com.vmturbo.mediation.conversion.util.TestUtils;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.SubDivisionData;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

public class AwsCloudDiscoveryConverterTest {

    private static final String AWS_ENGINEERING_FILE_PATH = AwsCloudDiscoveryConverterTest.class
        .getClassLoader().getResource("data/aws_engineering.aws.amazon.com.txt").getPath();

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

            // check VM buys TenancyAccess commodity from CT
            assertEquals(1, newVM.getCommoditiesBoughtList().stream()
                .filter(commodityBought -> commodityBought.getProviderType() ==
                    EntityType.COMPUTE_TIER)
                .flatMap(commodityBought -> commodityBought.getBoughtList().stream())
                .filter(commodityDTO -> commodityDTO.getCommodityType() == CommodityType.TENANCY_ACCESS)
                .count());

            // check VM buys 1 ZONE commodity from AZ
            assertEquals(1, newVM.getCommoditiesBoughtList().stream()
                    .filter(commodityBought -> commodityBought.getProviderType() ==
                            EntityType.AVAILABILITY_ZONE)
                    .flatMap(commodityBought -> commodityBought.getBoughtList().stream()
                            .filter(commodityDTO -> commodityDTO.getCommodityType() == CommodityType.ZONE
                                    && commodityDTO.getKey().equals(commodityBought.getProviderId())))
                    .count());

            // check 'active' for VM bought commodities not set to false
            newVM.getCommoditiesBoughtList().stream()
                    .flatMap(commodityBought -> commodityBought.getBoughtList().stream())
                    .filter(VirtualMachineConverter.COMMODITIES_TO_CLEAR_ACTIVE::contains)
                    .forEach(commodityDTO -> assertTrue(commodityDTO.getActive()));

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

            if (oldSubDivisionById.isEmpty() && vm.hasVirtualMachineData() &&
                    vm.getVirtualMachineData().getNumEphemeralStorages() == 0) {
                // not connected to volume
                assertNull(layeredOver.get(EntityType.VIRTUAL_VOLUME));
            } else {
                // connected to volume
                assertThat(layeredOver.get(EntityType.VIRTUAL_VOLUME).stream()
                                .filter(v -> v.hasVirtualVolumeData() &&
                                            !v.getVirtualVolumeData().getIsEphemeral())
                                .map(EntityDTO.Builder::getId).collect(Collectors.toList()),
                        containsInAnyOrder(oldSubDivisionById.keySet().toArray()));

                // check volumes
                layeredOver.get(EntityType.VIRTUAL_VOLUME).stream()
                        .filter(v -> v.hasVirtualVolumeData() &&
                                !v.getVirtualVolumeData().getIsEphemeral())
                        .forEach(volume -> {
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
                });
            }

            // check vm owned by BusinessAccount
            assertThat(ba.getConsistsOfList(), hasItem(vmId));

            // check if volumes are attached to VM with ephemeralStorage count > 0
            if (vm.hasVirtualMachineData() &&
                    vm.getVirtualMachineData().getNumEphemeralStorages() > 0) {
                final List<String> ephemeralVolumeIds =
                    layeredOver.get(EntityType.VIRTUAL_VOLUME).stream()
                        .filter(e -> e.hasVirtualVolumeData() &&
                            e.getVirtualVolumeData().getIsEphemeral())
                        .map(EntityDTO.Builder::getId)
                        .collect(Collectors.toList());
                assertTrue(ba.getConsistsOfList().containsAll(ephemeralVolumeIds));
                assertEquals(vm.getVirtualMachineData().getNumEphemeralStorages(),
                    ephemeralVolumeIds.size());
            }
        });
    }

    @Test
    public void testBusinessAccountConverter() {
        IEntityConverter converter = new BusinessAccountConverter(SDKProbeType.AWS);
        rawEntitiesByType.get(EntityType.BUSINESS_ACCOUNT).forEach(entity -> {
            String baId = entity.getId();
            EntityDTO oldEntity = awsConverter.getRawEntityDTO(baId);
            EntityDTO.Builder newEntity = awsConverter.getNewEntityBuilder(baId);

            // check ba not removed
            assertTrue(converter.convert(newEntity, awsConverter));

            // check unmodified fields
            assertEquals(oldEntity.getDisplayName(), newEntity.getDisplayName());

            // master account owns sub account
            if (newEntity.getId().equals(masterAccountId)) {
                assertThat(newEntity.getConsistsOfList(), containsInAnyOrder("323871187550",
                        "001844731978", "631949720430"));
            }

            // check that dataDiscovered field is cleared if it is false
            assertTrue(newEntity.hasBusinessAccountData());
            assertEquals(oldEntity.getBusinessAccountData().getDataDiscovered(),
                newEntity.getBusinessAccountData().hasDataDiscovered());
        });
    }

    @Test
    public void testReservedInstanceConverter() {
        IEntityConverter converter = new DefaultConverter();
        rawEntitiesByType.get(EntityType.RESERVED_INSTANCE).forEach(entity ->
                convertAndVerifyEntityUnmodified(converter, entity.getId()));
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
            Map<EntityType, EntityType> oldToNewProviderTypeMapping,
            List<EntityType> newEntityTypesWithNoMapping) {
        Object[] expectedNewProviderTypes = Stream.concat(TestUtils.getOldProviderTypes(oldEntity, awsConverter).stream()
                        .map(entityType -> oldToNewProviderTypeMapping.getOrDefault(entityType, entityType)),
                newEntityTypesWithNoMapping.stream()).toArray();
        assertThat(TestUtils.getNewProviderTypes(newEntity, awsConverter), containsInAnyOrder(expectedNewProviderTypes));
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
