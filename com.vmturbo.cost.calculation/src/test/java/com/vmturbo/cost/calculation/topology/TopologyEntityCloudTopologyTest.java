package com.vmturbo.cost.calculation.topology;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class TopologyEntityCloudTopologyTest {

    private static final String DEFAULT_NAME = "foo";

    private static final TopologyEntityDTO AZ = constructTopologyEntity("this is available",
        EntityType.AVAILABILITY_ZONE_VALUE)
        .build();

    private static final TopologyEntityDTO REGION = constructTopologyEntity("region", EntityType.REGION_VALUE)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
        .setConnectedEntityType(AZ.getEntityType())
        .setConnectedEntityId(AZ.getOid())
        .setConnectionType(ConnectionType.OWNS_CONNECTION))
        .build();

    private static final TopologyEntityDTO COMPUTE_TIER = constructTopologyEntity("computeTier",
        EntityType.COMPUTE_TIER_VALUE).addConnectedEntityList(ConnectedEntity.newBuilder()
        .setConnectedEntityType(REGION.getEntityType())
        .setConnectedEntityId(REGION.getOid())
        .setConnectionType(ConnectionType.NORMAL_CONNECTION))
        .build();

    private static final TopologyEntityDTO DATABASE_TIER = constructTopologyEntity("DatabaseTier",
        EntityType.DATABASE_TIER_VALUE)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(REGION.getEntityType())
            .setConnectedEntityId(REGION.getOid())
            .setConnectionType(ConnectionType.NORMAL_CONNECTION))
        .build();

    private static final TopologyEntityDTO DATABASE_SERVER_TIER = constructTopologyEntity("DatabaseServerTier",
        EntityType.DATABASE_SERVER_TIER_VALUE)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(REGION.getEntityType())
            .setConnectedEntityId(REGION.getOid())
            .setConnectionType(ConnectionType.NORMAL_CONNECTION))
        .build();

    private static final TopologyEntityDTO STORAGE_TIER = constructTopologyEntity("StorageTier",
        EntityType.STORAGE_TIER_VALUE)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(REGION.getEntityType())
            .setConnectedEntityId(REGION.getOid())
            .setConnectionType(ConnectionType.NORMAL_CONNECTION))
        .build();

    private static final TopologyEntityDTO EMPTY_STORAGE_TIER = TopologyEntityDTO.newBuilder()
        .setOid(EntityType.STORAGE_TIER_VALUE)
        .setEntityType(EntityType.STORAGE_TIER_VALUE)
        .build();

    private static final TopologyEntityDTO SERVICE = constructTopologyEntity("service",
        EntityType.CLOUD_SERVICE_VALUE)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityId(COMPUTE_TIER.getOid())
            .setConnectedEntityType(COMPUTE_TIER.getEntityType())
            .setConnectionType(ConnectionType.OWNS_CONNECTION))
        .build();

    private static final TopologyEntityDTO VOLUME = constructTopologyEntity("VirtualVolume",
        EntityType.VIRTUAL_VOLUME_VALUE)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(AZ.getEntityType())
            .setConnectedEntityId(AZ.getOid()))
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityId(STORAGE_TIER.getOid())
            .setConnectedEntityType(STORAGE_TIER.getEntityType()))
        .build();

    private static final TopologyEntityDTO VM = constructTopologyEntity(DEFAULT_NAME,
        EntityType.VIRTUAL_MACHINE_VALUE)
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderId(COMPUTE_TIER.getOid())
            .setProviderEntityType(COMPUTE_TIER.getEntityType()))
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderId(STORAGE_TIER.getOid())
            .setProviderEntityType(STORAGE_TIER.getEntityType())
            .setVolumeId(VOLUME.getOid()))
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
            .setVirtualMachine(VirtualMachineInfo.newBuilder()
                .setGuestOsInfo(OS.newBuilder()
                    .setGuestOsType(OSType.LINUX)
                    .setGuestOsName(OSType.LINUX.name()))
                .setTenancy(Tenancy.DEFAULT)))
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(AZ.getEntityType())
            .setConnectedEntityId(AZ.getOid()))
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(VOLUME.getEntityType())
            .setConnectedEntityId(VOLUME.getOid()))
        .build();

    private static final TopologyEntityDTO EMPTY_VM = constructTopologyEntity(DEFAULT_NAME,
        EntityType.VIRTUAL_MACHINE_VALUE)
        .build();

    private static final TopologyEntityDTO DATABASE = constructTopologyEntity(DEFAULT_NAME,
        EntityType.DATABASE_VALUE)
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderId(DATABASE_TIER.getOid())
            .setProviderEntityType(DATABASE_TIER.getEntityType()))
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
            .setDatabase(DatabaseInfo.newBuilder()
                .setEdition(DatabaseEdition.SQL_SERVER_EXPRESS)
                .setEngine(DatabaseEngine.MARIADB)))
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(AZ.getEntityType())
            .setConnectedEntityId(AZ.getOid()))
        .build();

    private static final TopologyEntityDTO DATABASE_SERVER = constructTopologyEntity(DEFAULT_NAME,
        EntityType.DATABASE_SERVER_VALUE)
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderId(DATABASE_SERVER_TIER.getOid())
            .setProviderEntityType(DATABASE_SERVER_TIER.getEntityType()))
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
            .setDatabase(DatabaseInfo.newBuilder()
                .setEdition(DatabaseEdition.SQL_SERVER_EXPRESS)
                .setEngine(DatabaseEngine.MARIADB)))
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(AZ.getEntityType())
            .setConnectedEntityId(AZ.getOid()))
        .build();

    private static final TopologyEntityDTO EMPTY_DATABASE = constructTopologyEntity(DEFAULT_NAME,
        EntityType.DATABASE_VALUE)
        .build();

    private static final TopologyEntityDTO EMPTY_DATABASE_SERVER = constructTopologyEntity(DEFAULT_NAME,
        EntityType.DATABASE_VALUE)
        .build();

    private static final TopologyEntityDTO BUSINESS_ACCOUNT = constructTopologyEntity("businessAccount",
        EntityType.BUSINESS_ACCOUNT_VALUE)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityId(VM.getOid())
            .setConnectedEntityType(VM.getEntityType())
            .setConnectionType(ConnectionType.OWNS_CONNECTION))
        .build();

    private static final Stream<TopologyEntityDTO> topologyStream =
        Stream.of(VM, EMPTY_VM, DATABASE, DATABASE_SERVER, EMPTY_DATABASE, EMPTY_DATABASE_SERVER, AZ, COMPUTE_TIER, DATABASE_TIER,
            DATABASE_SERVER_TIER, STORAGE_TIER, EMPTY_STORAGE_TIER, VOLUME, REGION, BUSINESS_ACCOUNT, SERVICE);

    private static final TopologyEntityCloudTopology cloudTopology = new TopologyEntityCloudTopology(topologyStream);
    private static long globalOid = 1000;

    private static long getNextOid(){
        return ++globalOid;
    }

    private static Builder constructTopologyEntity(String displayName, int eType){
        return TopologyEntityDTO.newBuilder()
            .setOid(getNextOid())
            .setDisplayName(displayName)
            .setEntityType(eType);
    }
    @Test
    public void testGetEntityOid() {
        assertThat(cloudTopology.getEntity(VM.getOid()), is(Optional.of(VM)));
    }

    @Test
    public void testGetEntityComputeTier() {
        assertThat(cloudTopology.getComputeTier(VM.getOid()), is(Optional.of(COMPUTE_TIER)));
    }

    @Test
    public void testGetEmptyEntityComputeTier() {
        Assert.assertFalse(cloudTopology.getComputeTier(EMPTY_VM.getOid()).isPresent());
    }

    @Test
    public void testGetEntityDatabaseTier() {
        assertThat(cloudTopology.getDatabaseTier(DATABASE.getOid()), is(Optional.of(DATABASE_TIER)));
    }

    @Test
    public void testGetEmptyEntityDatabaseTier() {
        Assert.assertFalse(cloudTopology.getDatabaseTier(EMPTY_DATABASE.getOid()).isPresent());
    }

    /**
     * Test getting the DB server tier of an entity.
     */
    @Test
    public void testGetEntityDatabaseServerTier() {
        assertThat(cloudTopology.getDatabaseServerTier(DATABASE_SERVER.getOid()), is(Optional.of(DATABASE_SERVER_TIER)));
    }

    /**
     * Test getting the DB server tier of a database not connected to a DB server tier.
     */
    @Test
    public void testGetEmptyEntityDatabaseServerTier() {
        Assert.assertFalse(cloudTopology.getDatabaseTier(EMPTY_DATABASE_SERVER.getOid()).isPresent());
    }

    @Test
    public void testGetEntityRegionViaAZ() {
        assertThat(cloudTopology.getConnectedRegion(VM.getOid()), is(Optional.of(REGION)));
    }

    @Test
    public void testGetEntityAZ() {
        assertThat(cloudTopology.getConnectedAvailabilityZone(VM.getOid()), is(Optional.of(AZ)));
    }

    @Test
    public void testGetRegionDirectly() {
        assertThat(cloudTopology.getConnectedRegion(COMPUTE_TIER.getOid()), is(Optional.of(REGION)));
    }

    @Test
    public void testGetOwnedBy() {
        assertThat(cloudTopology.getOwner(VM.getOid()), is(Optional.of(BUSINESS_ACCOUNT)));
    }

    @Test
    public void testGetService() {
        assertThat(cloudTopology.getConnectedService(COMPUTE_TIER.getOid()), is(Optional.of(SERVICE)));
    }

    @Test
    public void testGetServiceWithService() {
        assertThat(cloudTopology.getConnectedService(SERVICE.getOid()), is(Optional.of(SERVICE)));
    }

    @Test
    public void testGetConnectedVolumes() {
        assertThat(cloudTopology.getConnectedVolumes(VM.getOid()), is(Collections.singletonList(VOLUME)));
    }

    @Test
    public void testGetVmStorageTier() {
        assertThat(cloudTopology.getStorageTier(VM.getOid()).get().getEntityType(),
            is(Optional.of(STORAGE_TIER).get().getEntityType()));
    }

    @Test
    public void testGetVolumeStorageTier() {
        assertThat(cloudTopology.getStorageTier(VOLUME.getOid()).get().getEntityType(),
            is(Optional.of(STORAGE_TIER).get().getEntityType()));
    }

    @Test
    public void testGetEmptyEntityStorageTier() {
        Assert.assertFalse(cloudTopology.getStorageTier(EMPTY_STORAGE_TIER.getOid()).isPresent());
    }

    @Test
    public void testGetAllEntities() {
        final List<TopologyEntityDTO> allRegions = cloudTopology.getAllRegions();
        Assert.assertEquals(1, allRegions.size());
        final List<TopologyEntityDTO> allVMs = cloudTopology
            .getAllEntitiesOfType(EntityType.VIRTUAL_MACHINE_VALUE);
        Assert.assertEquals(2, allVMs.size());
        Set<Integer> entityTypeSet = Sets.newHashSet(EntityType.VIRTUAL_MACHINE_VALUE,
            EntityType.STORAGE_VALUE);
        final List<TopologyEntityDTO> allEntitesOfTypes = cloudTopology.getAllEntitiesOfType(entityTypeSet);
        Assert.assertEquals(2, allEntitesOfTypes.size());
        // testing the number of entities defined in the test
        Assert.assertEquals(16, cloudTopology.size());
    }
}
