package com.vmturbo.cost.calculation.topology;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Map;
import java.util.Optional;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
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

    private final TopologyEntityDTO BUSINESS_ACCOUNT = TopologyEntityDTO.newBuilder()
            .setOid(124L)
            .setDisplayName("businessAccount")
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                .setConnectedEntityId(VM.getOid())
                .setConnectedEntityType(VM.getEntityType())
                .setConnectionType(ConnectionType.OWNS_CONNECTION))
            .build();

    private final Map<Long, TopologyEntityDTO> topology = ImmutableMap.<Long, TopologyEntityDTO>builder()
            .put(VM.getOid(), VM)
            .put(AZ.getOid(), AZ)
            .put(COMPUTE_TIER.getOid(), COMPUTE_TIER)
            .put(REGION.getOid(), REGION)
            .put(BUSINESS_ACCOUNT.getOid(), BUSINESS_ACCOUNT)
            .put(SERVICE.getOid(), SERVICE)
            .build();

    private final TopologyEntityCloudTopologyFactory topologyFactory =
            TopologyEntityCloudTopology.newFactory();


    @Test
    public void testGetEntityOid() {
        final TopologyEntityCloudTopology cloudTopology = topologyFactory.newCloudTopology(topology);
        assertThat(cloudTopology.getEntity(VM.getOid()), is(Optional.of(VM)));
    }

    @Test
    public void testGetEntityComputeTier() {
        final TopologyEntityCloudTopology cloudTopology = topologyFactory.newCloudTopology(topology);
        assertThat(cloudTopology.getComputeTier(VM.getOid()), is(Optional.of(COMPUTE_TIER)));
    }

    @Test
    public void testGetEntityRegionViaAz() {
        final TopologyEntityCloudTopology cloudTopology = topologyFactory.newCloudTopology(topology);
        assertThat(cloudTopology.getConnectedRegion(VM.getOid()), is(Optional.of(REGION)));
    }

    @Test
    public void testGetRegionDirectly() {
        final TopologyEntityCloudTopology cloudTopology = topologyFactory.newCloudTopology(topology);
        assertThat(cloudTopology.getConnectedRegion(COMPUTE_TIER.getOid()), is(Optional.of(REGION)));
    }

    @Test
    public void testGetOwnedBy() {
        final TopologyEntityCloudTopology cloudTopology = topologyFactory.newCloudTopology(topology);
        assertThat(cloudTopology.getOwner(VM.getOid()), is(Optional.of(BUSINESS_ACCOUNT)));
    }

    @Test
    public void testGetService() {
        final TopologyEntityCloudTopology cloudTopology = topologyFactory.newCloudTopology(topology);
        assertThat(cloudTopology.getConnectedService(COMPUTE_TIER.getOid()), is(Optional.of(SERVICE)));
    }
}
