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
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class TopologyEntityCloudTopologyTest {

    private final TopologyEntityDTO REGION = TopologyEntityDTO.newBuilder()
            .setOid(9L)
            .setDisplayName("region")
            .setEntityType(EntityType.REGION_VALUE)
            .build();

    private final TopologyEntityDTO AZ = TopologyEntityDTO.newBuilder()
            .setOid(8L)
            .setDisplayName("this is available")
            .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityType(REGION.getEntityType())
                    .setConnectedEntityId(REGION.getOid()))
            .build();

    private final TopologyEntityDTO COMPUTE_TIER = TopologyEntityDTO.newBuilder()
            .setOid(99L)
            .setDisplayName("computeTier")
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
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

    private final Map<Long, TopologyEntityDTO> topology = ImmutableMap.<Long, TopologyEntityDTO>builder()
            .put(VM.getOid(), VM)
            .put(AZ.getOid(), AZ)
            .put(COMPUTE_TIER.getOid(), COMPUTE_TIER)
            .put(REGION.getOid(), REGION)
            .build();


    @Test
    public void testGetEntityOid() {
        final TopologyEntityCloudTopology cloudTopology = new TopologyEntityCloudTopology(topology);
        assertThat(cloudTopology.getEntity(VM.getOid()), is(Optional.of(VM)));
    }

    @Test
    public void testGetEntityComputeTier() {
        final TopologyEntityCloudTopology cloudTopology = new TopologyEntityCloudTopology(topology);
        assertThat(cloudTopology.getComputeTier(VM.getOid()), is(Optional.of(COMPUTE_TIER)));
    }

    @Test
    public void testGetEntityRegion() {
        final TopologyEntityCloudTopology cloudTopology = new TopologyEntityCloudTopology(topology);
        assertThat(cloudTopology.getRegion(VM.getOid()), is(Optional.of(REGION)));
    }
}
