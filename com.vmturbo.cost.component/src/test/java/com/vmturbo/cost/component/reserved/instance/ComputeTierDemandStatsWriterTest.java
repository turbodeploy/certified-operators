package com.vmturbo.cost.component.reserved.instance;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsWriter.ComputeTierDemandStatsRecord;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class ComputeTierDemandStatsWriterTest {

    private ComputeTierDemandStatsWriter computeTierDemandStatsWriter;

    private static final Long targetId = 1234L;

    @Test
    public void testStatsRecordtoCountMapping() {
        computeTierDemandStatsWriter = new ComputeTierDemandStatsWriter(Mockito.mock(ComputeTierDemandStatsStore.class),
                null, 0.0f);
        final Map<Long, Long> workloadIdToBusinessAccountIdMap = new HashMap<>();
        workloadIdToBusinessAccountIdMap.put(1L, targetId);
        workloadIdToBusinessAccountIdMap.put(2L, targetId);
        workloadIdToBusinessAccountIdMap.put(3L, targetId);
        workloadIdToBusinessAccountIdMap.put(4L, targetId);
        workloadIdToBusinessAccountIdMap.put(5L, targetId);
        workloadIdToBusinessAccountIdMap.put(6L, targetId);

        final Map<Long, TopologyEntityDTO> cloudEntities = new HashMap<>();
        cloudEntities.put(1L, getTopologyEntityDTO(1L, 1L,"vm1", OSType.LINUX, Tenancy.DEFAULT));
        cloudEntities.put(2L, getTopologyEntityDTO(2L, 1L,"vm2", OSType.LINUX, Tenancy.DEFAULT));
        cloudEntities.put(3L, getTopologyEntityDTO(3L, 1L,"vm3", OSType.WINDOWS, Tenancy.DEFAULT));
        cloudEntities.put(4L, getTopologyEntityDTO(4L, 2L,"vm4", OSType.LINUX, Tenancy.DEFAULT));
        cloudEntities.put(5L, getTopologyEntityDTO(5L, 2L,"vm5", OSType.LINUX, Tenancy.DEFAULT));
        cloudEntities.put(6L, getTopologyEntityDTO(6L, 2L,"vm6", OSType.LINUX, Tenancy.DEDICATED));

        final Map<ComputeTierDemandStatsRecord, Integer> statsRecordIntegerMap =
                computeTierDemandStatsWriter.getStatsRecordToCountMapping(cloudEntities, workloadIdToBusinessAccountIdMap);
        assertEquals(4, statsRecordIntegerMap.size());
        ComputeTierDemandStatsRecord record1 = new ComputeTierDemandStatsRecord(targetId, 1L, 0L,
                (byte)OSType.LINUX.getNumber(), (byte)Tenancy.DEFAULT.getNumber());
        ComputeTierDemandStatsRecord record2 = new ComputeTierDemandStatsRecord(targetId, 1L, 0L,
                (byte)OSType.WINDOWS.getNumber(), (byte)Tenancy.DEFAULT.getNumber());
        ComputeTierDemandStatsRecord record3 = new ComputeTierDemandStatsRecord(targetId, 2L, 0L,
                (byte)OSType.LINUX.getNumber(), (byte)Tenancy.DEFAULT.getNumber());
        ComputeTierDemandStatsRecord record4 = new ComputeTierDemandStatsRecord(targetId, 2L, 0L,
                (byte)OSType.LINUX.getNumber(), (byte)Tenancy.DEDICATED.getNumber());

        assertEquals((Integer)2, statsRecordIntegerMap.get(record1));
        assertEquals((Integer)1, statsRecordIntegerMap.get(record2));
        assertEquals((Integer)2, statsRecordIntegerMap.get(record3));
        assertEquals((Integer)1, statsRecordIntegerMap.get(record4));
    }

    private TopologyEntityDTO getTopologyEntityDTO(Long oid, Long computeTierProviderId,
                                                   String displayName, OSType osType, Tenancy tenancy) {
        return TopologyEntityDTO.newBuilder().setDisplayName(displayName)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(oid)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setVirtualMachine(VirtualMachineInfo.newBuilder()
                                .setGuestOsInfo(OS.newBuilder().setGuestOsType(osType).build())
                                .setTenancy(tenancy)
                                .build()).build())
                                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                                        .newBuilder().setProviderEntityType(EntityType.COMPUTE_TIER_VALUE)
                                        .setProviderId(computeTierProviderId)
                                        .build())
                                .addConnectedEntityList(ConnectedEntity.newBuilder()
                                        .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE).build())
                                .build();
    }
}
