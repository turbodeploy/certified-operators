package com.vmturbo.cost.calculation.topology;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.group.api.ImmutableGroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VMBillingType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class TopologyEntityCloudTopologyTest {

    private static final String DEFAULT_NAME = "foo";

    private static final long AZ_ID = 1000L;
    private static final long REGION_ID = 1001;
    private static final long COMPUTE_TIER_ID = 1002L;
    private static final long DB_TIER_ID = 1003L;
    private static final long DB_SERVER_TIER_ID = 1004L;
    private static final long STORAGE_TIER_ID = 1005L;
    private static final long EMPTY_STORAGE_TIER_ID = 1006L;
    private static final long SERVICE_ID = 1007L;
    private static final long VOLUME_ID = 1008L;
    private static final long VM_ID = 1009L;
    private static final long EMPTY_VM_ID = 1010L;
    private static final long DB_ID = 1011L;
    private static final long DB_SERVER_ID = 1012L;
    private static final long EMPTY_DB_ID = 1013L;
    private static final long EMPTY_DB_SERVER_ID = 1014L;
    private static final long ACCOUNT_ID = 1015L;
    private static final long BILLING_FAMILY_ID = 1016L;

    private static final TopologyEntityDTO AZ =
            constructTopologyEntity(AZ_ID, "this is available", EntityType.AVAILABILITY_ZONE_VALUE)
                    .build();

    private static final TopologyEntityDTO REGION =
            constructTopologyEntity(REGION_ID, "region", EntityType.REGION_VALUE)
                    .addConnectedEntityList(
                            ConnectedEntity.newBuilder()
                                .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                                .setConnectedEntityId(AZ_ID)
                                .setConnectionType(ConnectionType.OWNS_CONNECTION))
                    .build();

    private static final TopologyEntityDTO COMPUTE_TIER =
            constructTopologyEntity(COMPUTE_TIER_ID, "computeTier", EntityType.COMPUTE_TIER_VALUE)
                    .addConnectedEntityList(ConnectedEntity.newBuilder()
                                                .setConnectedEntityId(REGION_ID)
                                                .setConnectedEntityType(EntityType.REGION_VALUE)
                                                .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
                    .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setComputeTier(ComputeTierInfo.newBuilder()
                            .setNumCoupons(5).build()).build())
                    .build();

    private static final TopologyEntityDTO DATABASE_TIER =
            constructTopologyEntity(DB_TIER_ID, "DatabaseTier", EntityType.DATABASE_TIER_VALUE)
                    .addConnectedEntityList(ConnectedEntity.newBuilder()
                                                .setConnectedEntityId(REGION_ID)
                                                .setConnectedEntityType(EntityType.REGION_VALUE)
                                                .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
                    .build();

    private static final TopologyEntityDTO DATABASE_SERVER_TIER =
            constructTopologyEntity(DB_SERVER_TIER_ID, "DatabaseServerTier",
                                    EntityType.DATABASE_SERVER_TIER_VALUE)
                    .addConnectedEntityList(ConnectedEntity.newBuilder()
                                                .setConnectedEntityId(REGION_ID)
                                                .setConnectedEntityType(EntityType.REGION_VALUE)
                                                .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
                    .build();

    private static final TopologyEntityDTO STORAGE_TIER =
            constructTopologyEntity(STORAGE_TIER_ID, "StorageTier", EntityType.STORAGE_TIER_VALUE)
                    .addConnectedEntityList(ConnectedEntity.newBuilder()
                                                .setConnectedEntityId(REGION_ID)
                                                .setConnectedEntityType(EntityType.REGION_VALUE)
                                                .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
                    .build();

    private static final TopologyEntityDTO EMPTY_STORAGE_TIER =
            TopologyEntityDTO.newBuilder()
                .setOid(EMPTY_STORAGE_TIER_ID)
                .setEntityType(EntityType.STORAGE_TIER_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();

    private static final TopologyEntityDTO SERVICE =
            constructTopologyEntity(SERVICE_ID, "service", EntityType.CLOUD_SERVICE_VALUE)
                .addConnectedEntityList(
                        ConnectedEntity.newBuilder()
                            .setConnectedEntityId(COMPUTE_TIER_ID)
                            .setConnectedEntityType(EntityType.COMPUTE_TIER_VALUE)
                            .setConnectionType(ConnectionType.OWNS_CONNECTION))
                .build();

    private static final TopologyEntityDTO VOLUME =
            constructTopologyEntity(VOLUME_ID, "VirtualVolume", EntityType.VIRTUAL_VOLUME_VALUE)
                .addConnectedEntityList(
                        ConnectedEntity.newBuilder()
                            .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                            .setConnectedEntityId(AZ_ID))
                .addConnectedEntityList(
                        ConnectedEntity.newBuilder()
                            .setConnectedEntityId(STORAGE_TIER_ID)
                            .setConnectedEntityType(EntityType.STORAGE_TIER_VALUE))
                .build();

    private static final TopologyEntityDTO VM =
            constructTopologyEntity(VM_ID, DEFAULT_NAME, EntityType.VIRTUAL_MACHINE_VALUE)
                .setEntityState(EntityState.POWERED_ON)
                .addCommoditiesBoughtFromProviders(
                        CommoditiesBoughtFromProvider.newBuilder()
                            .setProviderId(COMPUTE_TIER_ID)
                            .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE))
                .addCommoditiesBoughtFromProviders(
                        CommoditiesBoughtFromProvider.newBuilder()
                            .setProviderId(STORAGE_TIER_ID)
                            .setProviderEntityType(EntityType.STORAGE_TIER_VALUE)
                            .setVolumeId(VOLUME_ID))
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                                            .setConnectedEntityId(AZ_ID)
                                            .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                                            .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
                .setTypeSpecificInfo(
                        TypeSpecificInfo.newBuilder()
                            .setVirtualMachine(VirtualMachineInfo.newBuilder()
                            .setGuestOsInfo(OS.newBuilder()
                            .setGuestOsType(OSType.LINUX)
                            .setGuestOsName(OSType.LINUX.name()))
                            .setTenancy(Tenancy.DEFAULT)))
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                                            .setConnectedEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                                            .setConnectedEntityId(VOLUME_ID))
                .build();

    private static final TopologyEntityDTO EMPTY_VM =
            constructTopologyEntity(EMPTY_VM_ID, DEFAULT_NAME, EntityType.VIRTUAL_MACHINE_VALUE)
                    .setEntityState(EntityState.POWERED_ON)
                    .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                            .setVirtualMachine(VirtualMachineInfo.newBuilder()
                                    .setBillingType(VMBillingType.BIDDING).build()).build())
                .build();

    private static final TopologyEntityDTO DATABASE =
            constructTopologyEntity(DB_ID, DEFAULT_NAME, EntityType.DATABASE_VALUE)
                .addCommoditiesBoughtFromProviders(
                        CommoditiesBoughtFromProvider.newBuilder()
                            .setProviderId(DB_TIER_ID)
                            .setProviderEntityType(EntityType.DATABASE_TIER_VALUE))
                .setTypeSpecificInfo(
                        TypeSpecificInfo.newBuilder()
                            .setDatabase(DatabaseInfo.newBuilder()
                            .setEdition(DatabaseEdition.EXPRESS)
                            .setEngine(DatabaseEngine.MARIADB)))
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                                            .setConnectedEntityId(AZ_ID)
                                            .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                                            .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
                .build();

    private static final TopologyEntityDTO DATABASE_SERVER =
            constructTopologyEntity(DB_SERVER_ID, DEFAULT_NAME, EntityType.DATABASE_SERVER_VALUE)
                .addCommoditiesBoughtFromProviders(
                        CommoditiesBoughtFromProvider.newBuilder()
                            .setProviderId(DB_SERVER_TIER_ID)
                            .setProviderEntityType(EntityType.DATABASE_SERVER_TIER_VALUE))
                .setTypeSpecificInfo(
                        TypeSpecificInfo.newBuilder()
                            .setDatabase(DatabaseInfo.newBuilder()
                            .setEdition(DatabaseEdition.EXPRESS)
                            .setEngine(DatabaseEngine.MARIADB)))
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                                            .setConnectedEntityId(AZ_ID)
                                            .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                                            .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
                .build();

    private static final TopologyEntityDTO EMPTY_DATABASE =
            constructTopologyEntity(EMPTY_DB_ID, DEFAULT_NAME, EntityType.DATABASE_VALUE)
                .build();

    private static final TopologyEntityDTO EMPTY_DATABASE_SERVER =
            constructTopologyEntity(EMPTY_DB_SERVER_ID, DEFAULT_NAME, EntityType.DATABASE_VALUE)
                .build();

    private static final TopologyEntityDTO BUSINESS_ACCOUNT =
            constructTopologyEntity(ACCOUNT_ID, "businessAccount", EntityType.BUSINESS_ACCOUNT_VALUE)
                .addConnectedEntityList(
                        ConnectedEntity.newBuilder()
                            .setConnectedEntityId(VM_ID)
                            .setConnectedEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                            .setConnectionType(ConnectionType.OWNS_CONNECTION))
                .build();

    private static final Stream<TopologyEntityDTO> TOPOLOGY_STREAM =
        Stream.of(VM, EMPTY_VM, DATABASE, DATABASE_SERVER, EMPTY_DATABASE, EMPTY_DATABASE_SERVER, AZ,
                  COMPUTE_TIER, DATABASE_TIER, DATABASE_SERVER_TIER, STORAGE_TIER, EMPTY_STORAGE_TIER,
                  VOLUME, REGION, BUSINESS_ACCOUNT, SERVICE);

    private static final List<GroupAndMembers> BILLING_FAMILY_GROUPS =
            Collections.singletonList(ImmutableGroupAndMembers.builder()
                    .group(Grouping.newBuilder()
                            .setId(BILLING_FAMILY_ID)
                            .build())
                    .entities(Collections.singleton(ACCOUNT_ID))
                    .members(Collections.singleton(ACCOUNT_ID))
                    .build());

    private static final GroupMemberRetriever groupMemberRetriever = mockGroupMemberRetriever();

    private static final TopologyEntityCloudTopology CLOUD_TOPOLOGY =
            new TopologyEntityCloudTopologyFactory
                    .DefaultTopologyEntityCloudTopologyFactory(groupMemberRetriever)
                    .newCloudTopology(TOPOLOGY_STREAM);

    private static GroupMemberRetriever mockGroupMemberRetriever() {
        final GroupMemberRetriever groupMemberRetriever = mock(GroupMemberRetriever.class);
        when(groupMemberRetriever.getGroupsWithMembers(any())).thenReturn(BILLING_FAMILY_GROUPS);
        return groupMemberRetriever;
    }

    private static Builder constructTopologyEntity(long oid, String displayName, int eType) {
        return TopologyEntityDTO.newBuilder()
            .setOid(oid)
            .setDisplayName(displayName)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setEntityType(eType);
    }

    @Test
    public void testGetEntityOid() {
        assertThat(CLOUD_TOPOLOGY.getEntity(VM.getOid()), is(Optional.of(VM)));
    }

    @Test
    public void testGetEntityComputeTier() {
        assertThat(CLOUD_TOPOLOGY.getComputeTier(VM.getOid()), is(Optional.of(COMPUTE_TIER)));
    }

    @Test
    public void testGetEmptyEntityComputeTier() {
        Assert.assertFalse(CLOUD_TOPOLOGY.getComputeTier(EMPTY_VM.getOid()).isPresent());
    }

    @Test
    public void testGetEntityDatabaseTier() {
        assertThat(CLOUD_TOPOLOGY.getDatabaseTier(DATABASE.getOid()), is(Optional.of(DATABASE_TIER)));
    }

    @Test
    public void testGetEmptyEntityDatabaseTier() {
        Assert.assertFalse(CLOUD_TOPOLOGY.getDatabaseTier(EMPTY_DATABASE.getOid()).isPresent());
    }

    /**
     * Test getting the DB server tier of an entity.
     */
    @Test
    public void testGetEntityDatabaseServerTier() {
        assertThat(CLOUD_TOPOLOGY.getDatabaseServerTier(DATABASE_SERVER.getOid()), is(Optional.of(DATABASE_SERVER_TIER)));
    }

    /**
     * Test getting the DB server tier of a database not connected to a DB server tier.
     */
    @Test
    public void testGetEmptyEntityDatabaseServerTier() {
        Assert.assertFalse(CLOUD_TOPOLOGY.getDatabaseTier(EMPTY_DATABASE_SERVER.getOid()).isPresent());
    }

    @Test
    public void testGetEntityRegionViaAZ() {
        assertThat(CLOUD_TOPOLOGY.getConnectedRegion(VM.getOid()), is(Optional.of(REGION)));
    }

    @Test
    public void testGetEntityAZ() {
        assertThat(CLOUD_TOPOLOGY.getConnectedAvailabilityZone(VM.getOid()), is(Optional.of(AZ)));
    }

    @Test
    public void testGetRegionDirectly() {
        assertThat(CLOUD_TOPOLOGY.getConnectedRegion(COMPUTE_TIER.getOid()), is(Optional.of(REGION)));
    }

    @Test
    public void testGetOwnedBy() {
        assertThat(CLOUD_TOPOLOGY.getOwner(VM.getOid()), is(Optional.of(BUSINESS_ACCOUNT)));
    }

    @Test
    public void testGetService() {
        assertThat(CLOUD_TOPOLOGY.getConnectedService(COMPUTE_TIER.getOid()), is(Optional.of(SERVICE)));
    }

    @Test
    public void testGetServiceWithService() {
        assertThat(CLOUD_TOPOLOGY.getConnectedService(SERVICE.getOid()), is(Optional.of(SERVICE)));
    }

    @Test
    public void testGetConnectedVolumes() {
        assertThat(CLOUD_TOPOLOGY.getConnectedVolumes(VM.getOid()), is(Collections.singletonList(VOLUME)));
    }

    @Test
    public void testGetVmStorageTier() {
        assertThat(CLOUD_TOPOLOGY.getStorageTier(VM.getOid()).get().getEntityType(),
            is(Optional.of(STORAGE_TIER).get().getEntityType()));
    }

    @Test
    public void testGetVolumeStorageTier() {
        assertThat(CLOUD_TOPOLOGY.getStorageTier(VOLUME.getOid()).get().getEntityType(),
            is(Optional.of(STORAGE_TIER).get().getEntityType()));
    }

    @Test
    public void testGetEmptyEntityStorageTier() {
        Assert.assertFalse(CLOUD_TOPOLOGY.getStorageTier(EMPTY_STORAGE_TIER.getOid()).isPresent());
    }

    @Test
    public void testGetAllEntities() {
        final List<TopologyEntityDTO> allRegions = CLOUD_TOPOLOGY.getAllRegions();
        assertEquals(1, allRegions.size());
        final List<TopologyEntityDTO> allVMs = CLOUD_TOPOLOGY
            .getAllEntitiesOfType(EntityType.VIRTUAL_MACHINE_VALUE);
        assertEquals(2, allVMs.size());
        Set<Integer> entityTypeSet = Sets.newHashSet(EntityType.VIRTUAL_MACHINE_VALUE,
            EntityType.STORAGE_VALUE);
        final List<TopologyEntityDTO> allEntitesOfTypes = CLOUD_TOPOLOGY.getAllEntitiesOfType(entityTypeSet);
        assertEquals(2, allEntitesOfTypes.size());
        // testing the number of entities defined in the test
        assertEquals(16, CLOUD_TOPOLOGY.size());
    }

    @Test
    public void testGetRICoverageCapacityForEntity() {
        long result = CLOUD_TOPOLOGY.getRICoverageCapacityForEntity(VM_ID);
        assertEquals(5, result);
        // VM with billing type as BIDDING.
        result = CLOUD_TOPOLOGY.getRICoverageCapacityForEntity(EMPTY_VM_ID);
        assertEquals(0, result);
    }

    /**
     * Test that correct billing family group is returned for Business Account.
     */
    @Test
    public void testGetBillingFamilyGroupForAccount() {
        final Optional<GroupAndMembers> billingFamilyGroup = CLOUD_TOPOLOGY
                .getBillingFamilyForEntity(ACCOUNT_ID);
        Assert.assertTrue(billingFamilyGroup.isPresent());
        final long billingFamilyId = billingFamilyGroup.map(group -> group.group().getId())
                .orElse(-1L);
        Assert.assertEquals(BILLING_FAMILY_ID, billingFamilyId);
    }

    /**
     * Test that correct billing family group is returned by VM.
     */
    @Test
    public void testGetBillingFamilyGroupForVM() {
        final Optional<GroupAndMembers> billingFamilyGroup = CLOUD_TOPOLOGY
                .getBillingFamilyForEntity(VM_ID);
        Assert.assertTrue(billingFamilyGroup.isPresent());
        final long billingFamilyId = billingFamilyGroup.map(group -> group.group().getId())
                .orElse(-1L);
        Assert.assertEquals(BILLING_FAMILY_ID, billingFamilyId);
    }

    /**
     * Test that empty Optional is returned if ownedBy is not found for an entity.
     */
    @Test
    public void testGetBillingFamilyGroupForEntityWithoutAccount() {
        final Optional<GroupAndMembers> billingFamilyGroup = CLOUD_TOPOLOGY
                .getBillingFamilyForEntity(DB_ID);
        Assert.assertFalse(billingFamilyGroup.isPresent());
    }
}
