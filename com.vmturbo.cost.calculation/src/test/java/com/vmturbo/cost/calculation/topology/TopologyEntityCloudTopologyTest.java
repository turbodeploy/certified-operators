package com.vmturbo.cost.calculation.topology;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
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

    private final TopologyEntityDTO AZ = TopologyEntityDTO.newBuilder()
            .setOid(8L)
            .setDisplayName("this is available")
            .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
            .build();

    private final TopologyEntityDTO REGION = TopologyEntityDTO.newBuilder()
            .setOid(9L)
            .setDisplayName("region")
            .setEntityType(EntityType.REGION_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityType(AZ.getEntityType())
                    .setConnectedEntityId(AZ.getOid())
                    .setConnectionType(ConnectionType.OWNS_CONNECTION))
            .build();

    private final TopologyEntityDTO COMPUTE_TIER = TopologyEntityDTO.newBuilder()
            .setOid(99L)
            .setDisplayName("computeTier")
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityType(REGION.getEntityType())
                    .setConnectedEntityId(REGION.getOid())
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION))
            .build();

    private final TopologyEntityDTO DATABASE_TIER = TopologyEntityDTO.newBuilder()
            .setOid(100L)
            .setDisplayName("DatabaseTier")
            .setEntityType(EntityType.DATABASE_TIER_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityType(REGION.getEntityType())
                    .setConnectedEntityId(REGION.getOid())
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION))
            .build();

    private final TopologyEntityDTO SERVICE = TopologyEntityDTO.newBuilder()
            .setOid(123L)
            .setDisplayName("service")
            .setEntityType(EntityType.CLOUD_SERVICE_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                .setConnectedEntityId(COMPUTE_TIER.getOid())
                .setConnectedEntityType(COMPUTE_TIER.getEntityType())
                .setConnectionType(ConnectionType.OWNS_CONNECTION))
            .build();

    private final TopologyEntityDTO VM = TopologyEntityDTO.newBuilder()
            .setOid(7L)
            .setDisplayName("foo")
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(COMPUTE_TIER.getOid())
                    .setProviderEntityType(COMPUTE_TIER.getEntityType()))
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualMachine(VirtualMachineInfo.newBuilder()
                            .setGuestOsType(OSType.LINUX)
                            .setTenancy(Tenancy.DEFAULT)))
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityType(AZ.getEntityType())
                    .setConnectedEntityId(AZ.getOid()))
            .build();

    private final TopologyEntityDTO DATABASE = TopologyEntityDTO.newBuilder()
            .setOid(10L)
            .setDisplayName("foo")
            .setEntityType(EntityType.DATABASE_VALUE)
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

    private final TopologyEntityDTO BUSINESS_ACCOUNT = TopologyEntityDTO.newBuilder()
            .setOid(124L)
            .setDisplayName("businessAccount")
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                .setConnectedEntityId(VM.getOid())
                .setConnectedEntityType(VM.getEntityType())
                .setConnectionType(ConnectionType.OWNS_CONNECTION))
            .build();

    private final Stream<TopologyEntityDTO> topologyStream =
            Stream.of(VM, DATABASE, AZ, COMPUTE_TIER, DATABASE_TIER, REGION, BUSINESS_ACCOUNT, SERVICE);

    @Test
    public void testGetEntityOid() {
        final TopologyEntityCloudTopology cloudTopology = new TopologyEntityCloudTopology(topologyStream);
        assertThat(cloudTopology.getEntity(VM.getOid()), is(Optional.of(VM)));
    }

    @Test
    public void testGetEntityComputeTier() {
        final TopologyEntityCloudTopology cloudTopology = new TopologyEntityCloudTopology(topologyStream);
        assertThat(cloudTopology.getComputeTier(VM.getOid()), is(Optional.of(COMPUTE_TIER)));
    }

    @Test
    public void testGetEntityDatabaseTier() {
        final TopologyEntityCloudTopology cloudTopology = new TopologyEntityCloudTopology(topologyStream);
        assertThat(cloudTopology.getDatabaseTier(DATABASE.getOid()), is(Optional.of(DATABASE_TIER)));
    }

    @Test
    public void testGetEntityRegionViaAz() {
        final TopologyEntityCloudTopology cloudTopology = new TopologyEntityCloudTopology(topologyStream);
        assertThat(cloudTopology.getConnectedRegion(VM.getOid()), is(Optional.of(REGION)));
    }

    @Test
    public void testGetEntityAZ() {
        final TopologyEntityCloudTopology cloudTopology = new TopologyEntityCloudTopology(topologyStream);
        assertThat(cloudTopology.getConnectedAvailabilityZone(VM.getOid()), is(Optional.of(AZ)));
    }

    @Test
    public void testGetRegionDirectly() {
        final TopologyEntityCloudTopology cloudTopology = new TopologyEntityCloudTopology(topologyStream);
        assertThat(cloudTopology.getConnectedRegion(COMPUTE_TIER.getOid()), is(Optional.of(REGION)));
    }

    @Test
    public void testGetOwnedBy() {
        final TopologyEntityCloudTopology cloudTopology = new TopologyEntityCloudTopology(topologyStream);
        assertThat(cloudTopology.getOwner(VM.getOid()), is(Optional.of(BUSINESS_ACCOUNT)));
    }

    @Test
    public void testGetService() {
        final TopologyEntityCloudTopology cloudTopology = new TopologyEntityCloudTopology(topologyStream);
        assertThat(cloudTopology.getConnectedService(COMPUTE_TIER.getOid()), is(Optional.of(SERVICE)));
    }

    @Test
    public void testGetServiceWithService() {
        final TopologyEntityCloudTopology cloudTopology = new TopologyEntityCloudTopology(topologyStream);
        assertThat(cloudTopology.getConnectedService(SERVICE.getOid()), is(Optional.of(SERVICE)));
    }
}
