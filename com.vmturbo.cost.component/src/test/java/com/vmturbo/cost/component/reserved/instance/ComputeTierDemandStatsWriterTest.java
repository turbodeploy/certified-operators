package com.vmturbo.cost.component.reserved.instance;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsWriter.ComputeTierDemandStatsRecord;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Test class for ComputeTierDemandStatsWriter.
 */
public class ComputeTierDemandStatsWriterTest {

    private ComputeTierDemandStatsWriter computeTierDemandStatsWriter;

    private final AtomicLong oidProvider = new AtomicLong();

    private final TopologyEntityCloudTopologyFactory cloudTopologyFactory =
            new DefaultTopologyEntityCloudTopologyFactory();

    private final TopologyEntityDTO availabilityZone = TopologyEntityDTO.newBuilder()
            .setOid(oidProvider.incrementAndGet())
            .setDisplayName("availability_zone")
            .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .build();

    private final TopologyEntityDTO region = TopologyEntityDTO.newBuilder()
            .setOid(oidProvider.incrementAndGet())
            .setDisplayName("region")
            .setEntityType(EntityType.REGION_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(availabilityZone.getOid())
                    .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                    .setConnectionType(ConnectionType.OWNS_CONNECTION))
            .build();

    /**
     * Tests ComputeTierDemandStatsWriter::statsRecordtoCountMapping.
     */
    @Test
    public void testStatsRecordtoCountMapping() {
        List<TopologyEntityDTO> entityDTOs = new ArrayList<>();
        final TopologyEntityDTO computeTierDTO1 = buildComputeTierDTO();
        final TopologyEntityDTO computeTierDTO2 = buildComputeTierDTO();
        entityDTOs.add(computeTierDTO1);
        entityDTOs.add(computeTierDTO2);
        TopologyEntityDTO vm1 = buildVMDTO(computeTierDTO1.getOid(), "vm1", OSType.LINUX,
                Tenancy.DEFAULT, EntityState.POWERED_ON);
        TopologyEntityDTO vm2 = buildVMDTO(computeTierDTO1.getOid(), "vm2", OSType.LINUX,
                Tenancy.DEFAULT, EntityState.POWERED_ON);
        TopologyEntityDTO vm3 = buildVMDTO(computeTierDTO1.getOid(), "vm3", OSType.WINDOWS,
                Tenancy.DEFAULT, EntityState.POWERED_ON);
        TopologyEntityDTO vm4 = buildVMDTO(computeTierDTO2.getOid(), "vm4", OSType.LINUX,
                Tenancy.DEFAULT, EntityState.POWERED_ON);
        TopologyEntityDTO vm5 = buildVMDTO(computeTierDTO2.getOid(), "vm5", OSType.LINUX,
                Tenancy.DEFAULT, EntityState.POWERED_ON);
        TopologyEntityDTO vm6 = buildVMDTO(computeTierDTO2.getOid(), "vm6", OSType.LINUX,
                Tenancy.DEDICATED, EntityState.POWERED_ON);
        // Demand for this VM will not be counted as it is powered off.
        TopologyEntityDTO vm7 = buildVMDTO(computeTierDTO1.getOid(), "vm7", OSType.LINUX,
                Tenancy.DEFAULT, EntityState.POWERED_OFF);
        List<TopologyEntityDTO> vmDTOs = Lists.newArrayList(vm1, vm2, vm3, vm4, vm5, vm6, vm7);
        final List<ConnectedEntity> connectedEntities = getConnectedEntities(vmDTOs);
        TopologyEntityDTO businessAccount = buildBusinessAccountDTO(connectedEntities);
        entityDTOs.addAll(vmDTOs);
        entityDTOs.add(businessAccount);
        entityDTOs.add(availabilityZone);
        entityDTOs.add(region);


        final CloudTopology<TopologyEntityDTO> topologyEntityDTOCloudTopology =
                generateCloudTopology(entityDTOs);

        final ProjectedRICoverageAndUtilStore projectedRICoverageAndUtilStore =
                Mockito.mock(ProjectedRICoverageAndUtilStore.class);
        Map<Long, Map<Long, Double>> projectedEntitiesRICoverages = new HashMap<>();
        Map<Long, Double> vm2Coverage = new HashMap<>();
        vm2Coverage.put(1L, 1D);
        Map<Long, Double> vm4Coverage = new HashMap<>();
        vm4Coverage.put(1L, 1D);
        projectedEntitiesRICoverages.put(vm2.getOid(), vm2Coverage);
        projectedEntitiesRICoverages.put(vm4.getOid(), vm4Coverage);

        Mockito.when(projectedRICoverageAndUtilStore.getAllProjectedEntitiesRICoverages())
                .thenReturn(projectedEntitiesRICoverages);
        computeTierDemandStatsWriter = new
                        ComputeTierDemandStatsWriter(Mockito.mock(ComputeTierDemandStatsStore.class),
                                        projectedRICoverageAndUtilStore, 0.0f);

        final Map<ComputeTierDemandStatsRecord, Integer> statsRecordIntegerMap =
                computeTierDemandStatsWriter.getStatsRecordToCountMapping(topologyEntityDTOCloudTopology, true);

        assertEquals(4, statsRecordIntegerMap.size());

        final ComputeTierDemandStatsRecord record1 = new ComputeTierDemandStatsRecord(
                businessAccount.getOid(), computeTierDTO1.getOid(), availabilityZone.getOid(),
                (byte)OSType.LINUX.getNumber(), (byte)Tenancy.DEFAULT.getNumber());
        final ComputeTierDemandStatsRecord record2 = new ComputeTierDemandStatsRecord(
                businessAccount.getOid(), computeTierDTO1.getOid(), availabilityZone.getOid(),
                (byte)OSType.WINDOWS.getNumber(), (byte)Tenancy.DEFAULT.getNumber());
        final ComputeTierDemandStatsRecord record3 = new ComputeTierDemandStatsRecord(
                businessAccount.getOid(), computeTierDTO2.getOid(), availabilityZone.getOid(),
                (byte)OSType.LINUX.getNumber(), (byte)Tenancy.DEFAULT.getNumber());
        final ComputeTierDemandStatsRecord record4 = new ComputeTierDemandStatsRecord(
                businessAccount.getOid(), computeTierDTO2.getOid(), availabilityZone.getOid(),
                (byte)OSType.LINUX.getNumber(), (byte)Tenancy.DEDICATED.getNumber());

        assertEquals((Integer)1, statsRecordIntegerMap.get(record1));
        assertEquals((Integer)1, statsRecordIntegerMap.get(record2));
        assertEquals((Integer)1, statsRecordIntegerMap.get(record3));
        assertEquals((Integer)1, statsRecordIntegerMap.get(record4));
    }

    private List<ConnectedEntity> getConnectedEntities(List<TopologyEntityDTO> vmDTOs) {
        final List<ConnectedEntity> connectedEntities = new ArrayList<>();
        for (TopologyEntityDTO vmDTO : vmDTOs) {
            final ConnectedEntity connectedEntity = ConnectedEntity.newBuilder()
                    .setConnectedEntityId(vmDTO.getOid())
                    .setConnectedEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .setConnectionType(ConnectionType.OWNS_CONNECTION).build();
            connectedEntities.add(connectedEntity);
        }
        return connectedEntities;
    }

    private TopologyEntityDTO buildBusinessAccountDTO(List<ConnectedEntity> connectedEntities) {
        return TopologyEntityDTO.newBuilder()
                .setOid(oidProvider.incrementAndGet())
                .setDisplayName("business_account")
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .addAllConnectedEntityList(connectedEntities)
                .build();
    }

    private TopologyEntityDTO buildVMDTO(Long computeTierProviderId, String displayName,
                                         OSType osType, Tenancy tenancy, EntityState entityState) {
        return TopologyEntityDTO.newBuilder()
                .setOid(oidProvider.incrementAndGet())
                .setDisplayName(displayName)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setVirtualMachine(VirtualMachineInfo.newBuilder()
                                .setGuestOsInfo(OS.newBuilder()
                                        .setGuestOsType(osType))
                                .setTenancy(tenancy)))
                .setEntityState(entityState)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE)
                        .setProviderId(computeTierProviderId))
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(availabilityZone.getOid())
                        .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                        .setConnectionType(ConnectionType.NORMAL_CONNECTION))
                .build();
    }

    /**
     * Returns a compute tier DTO.
     * @return returns a compute tier DTO.
     */
    private TopologyEntityDTO buildComputeTierDTO() {
        return TopologyEntityDTO.newBuilder()
                .setOid(oidProvider.incrementAndGet())
                .setDisplayName("computeTier")
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setComputeTier(ComputeTierInfo.newBuilder()
                                .setFamily("familyA")
                                .setNumCoupons(1)))
                .build();
    }

    /**
     * Given a List of DTO's creates a cloud topology.
     * @param entityDtos List of DTO's.
     * @return cloud topology.
     */
    private CloudTopology<TopologyEntityDTO> generateCloudTopology(
            List<TopologyEntityDTO> entityDtos) {
        return cloudTopologyFactory.newCloudTopology(entityDtos.stream());
    }
}
