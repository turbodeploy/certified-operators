package com.vmturbo.cost.component.reserved.instance;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.component.db.tables.records.ComputeTierTypeHourlyByWeekRecord;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsWriter.ComputeTierDemandStatsRecord;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsWriter.WeightedCounts;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VMBillingType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Test class for ComputeTierDemandStatsWriter.
 */
public class ComputeTierDemandStatsWriterTest {

    private ComputeTierDemandStatsWriter computeTierDemandStatsWriter;

    private final AtomicLong oidProvider = new AtomicLong();

    private final TopologyEntityCloudTopologyFactory cloudTopologyFactory =
            new DefaultTopologyEntityCloudTopologyFactory(mock(GroupMemberRetriever.class));

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
                Tenancy.DEFAULT, EntityState.POWERED_ON, VMBillingType.ONDEMAND);
        TopologyEntityDTO vm2 = buildVMDTO(computeTierDTO1.getOid(), "vm2", OSType.LINUX,
                Tenancy.DEFAULT, EntityState.POWERED_ON, VMBillingType.ONDEMAND);
        TopologyEntityDTO vm3 = buildVMDTO(computeTierDTO1.getOid(), "vm3", OSType.WINDOWS,
                Tenancy.DEFAULT, EntityState.POWERED_ON, VMBillingType.ONDEMAND);
        TopologyEntityDTO vm4 = buildVMDTO(computeTierDTO2.getOid(), "vm4", OSType.LINUX,
                Tenancy.DEFAULT, EntityState.POWERED_ON, VMBillingType.ONDEMAND);
        TopologyEntityDTO vm5 = buildVMDTO(computeTierDTO2.getOid(), "vm5", OSType.LINUX,
                Tenancy.DEFAULT, EntityState.POWERED_ON, VMBillingType.ONDEMAND);
        TopologyEntityDTO vm6 = buildVMDTO(computeTierDTO2.getOid(), "vm6", OSType.LINUX,
                Tenancy.DEDICATED, EntityState.POWERED_ON, VMBillingType.ONDEMAND);
        // Demand for this VM will not be counted as it is powered off.
        TopologyEntityDTO vm7 = buildVMDTO(computeTierDTO1.getOid(), "vm7", OSType.LINUX,
                Tenancy.DEFAULT, EntityState.POWERED_OFF, VMBillingType.ONDEMAND);
        // Demand for this VM will not be counted as its billing type is bidding.
        TopologyEntityDTO vm8 = buildVMDTO(computeTierDTO1.getOid(), "vm8", OSType.LINUX,
                Tenancy.DEFAULT, EntityState.POWERED_OFF, VMBillingType.BIDDING);
        List<TopologyEntityDTO> vmDTOs = Lists.newArrayList(vm1, vm2, vm3, vm4, vm5, vm6, vm7, vm8);
        final List<ConnectedEntity> connectedEntities = getConnectedEntities(vmDTOs);
        TopologyEntityDTO businessAccount = buildBusinessAccountDTO(connectedEntities);
        entityDTOs.addAll(vmDTOs);
        entityDTOs.add(businessAccount);
        entityDTOs.add(availabilityZone);
        entityDTOs.add(region);


        final CloudTopology<TopologyEntityDTO> topologyEntityDTOCloudTopology =
                generateCloudTopology(entityDTOs);

        final ProjectedRICoverageAndUtilStore projectedRICoverageAndUtilStore =
                mock(ProjectedRICoverageAndUtilStore.class);
        Map<Long, EntityReservedInstanceCoverage> projectedEntitiesRICoverages = new HashMap<>();

        Map<Long, Double> vm2Coverage = new HashMap<>();
        vm2Coverage.put(1L, 1D);
        EntityReservedInstanceCoverage coverage2 = EntityReservedInstanceCoverage.newBuilder()
                .putAllCouponsCoveredByRi(vm2Coverage).build();
        Map<Long, Double> vm4Coverage = new HashMap<>();
        vm4Coverage.put(1L, 1D);
        EntityReservedInstanceCoverage coverage4 = EntityReservedInstanceCoverage.newBuilder()
                .putAllCouponsCoveredByRi(vm4Coverage).build();
        projectedEntitiesRICoverages.put(vm2.getOid(), coverage2);
        projectedEntitiesRICoverages.put(vm4.getOid(), coverage4);

        Mockito.when(projectedRICoverageAndUtilStore.getAllProjectedEntitiesRICoverages())
                .thenReturn(projectedEntitiesRICoverages);
        computeTierDemandStatsWriter = new
                        ComputeTierDemandStatsWriter(mock(ComputeTierDemandStatsStore.class),
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
                                         OSType osType, Tenancy tenancy, EntityState entityState,
                                         VMBillingType vmBillingType) {
        return TopologyEntityDTO.newBuilder()
                .setOid(oidProvider.incrementAndGet())
                .setDisplayName(displayName)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setVirtualMachine(VirtualMachineInfo.newBuilder()
                                .setGuestOsInfo(OS.newBuilder()
                                        .setGuestOsType(osType))
                                .setTenancy(tenancy)
                                .setBillingType(vmBillingType)))
                .setEntityState(entityState)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE)
                        .setProviderId(computeTierProviderId))
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(availabilityZone.getOid())
                        .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                        .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
                .build();
    }

    /**
     * Tests the demand stats calculation.
     */
    @Test
    public void testUpdateStaleStatsRecords() {

        TopologyEntityDTO businessAccount = buildBusinessAccountDTO(Collections.emptyList());
        final TopologyEntityDTO computeTierDTO1 = buildComputeTierDTO();
        final TopologyEntityDTO computeTierDTO2 = buildComputeTierDTO();
        final ComputeTierDemandStatsRecord record1 = new ComputeTierDemandStatsRecord(
                businessAccount.getOid(), computeTierDTO1.getOid(), availabilityZone.getOid(),
                (byte)OSType.LINUX.getNumber(), (byte)Tenancy.DEFAULT.getNumber());
        final ComputeTierDemandStatsRecord record2 = new ComputeTierDemandStatsRecord(
                businessAccount.getOid(), computeTierDTO2.getOid(), availabilityZone.getOid(),
                (byte)OSType.LINUX.getNumber(), (byte)Tenancy.DEFAULT.getNumber());
        final ComputeTierDemandStatsRecord record3 = new ComputeTierDemandStatsRecord(
                businessAccount.getOid(), computeTierDTO2.getOid(), availabilityZone.getOid(),
                (byte)OSType.WINDOWS.getNumber(), (byte)Tenancy.DEFAULT.getNumber());
        final ComputeTierDemandStatsRecord record4 = new ComputeTierDemandStatsRecord(
                businessAccount.getOid(), computeTierDTO2.getOid(), availabilityZone.getOid(),
                (byte)OSType.LINUX.getNumber(), (byte)Tenancy.DEDICATED.getNumber());

        WeightedCounts count = mock(WeightedCounts.class);
        Mockito.when(count.getCountFromProjectedTopology()).thenReturn(new BigDecimal(1.0));
        Mockito.when(count.getCountFromSourceTopology()).thenReturn(new BigDecimal(1.0));


        final Map<ComputeTierDemandStatsRecord, WeightedCounts> existingRecords =
                ImmutableMap.of(record1, count, record2, count, record3, count, record4, count);

        final Map<ComputeTierDemandStatsRecord, Integer> newRecords =
                ImmutableMap.of(record1, 1, record2, 1, record4, 1);
        computeTierDemandStatsWriter = new
                ComputeTierDemandStatsWriter(mock(ComputeTierDemandStatsStore.class),
                mock(ProjectedRICoverageAndUtilStore.class), 0.6f);
        final Set<ComputeTierTypeHourlyByWeekRecord> updatedRecords =
                computeTierDemandStatsWriter.getComputeTierTypeHourlyByWeekRecords(newRecords,
                        existingRecords,  "Live",
                        false, 8, 10);
        Assert.assertTrue(updatedRecords.size() == 4);
        for (ComputeTierTypeHourlyByWeekRecord newStatsRecord: updatedRecords) {
            // the WINDOWS record is the only one not in the new topology.
            if (newStatsRecord.getPlatform() == (byte)OSType.WINDOWS.getNumber()) {
                Assert.assertTrue(newStatsRecord.getCountFromSourceTopology().floatValue() < 1f);
            } else {
                Assert.assertEquals(newStatsRecord.getCountFromSourceTopology().floatValue(),
                        1.0f, 0.0001);
            }
        }

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
