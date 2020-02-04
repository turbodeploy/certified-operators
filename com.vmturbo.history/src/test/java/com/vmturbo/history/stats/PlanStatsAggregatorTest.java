package com.vmturbo.history.stats;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.schema.abstraction.tables.records.MktSnapshotsStatsRecord;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test cases for {@link PlanStatsAggregator}.
 *
 */
public class PlanStatsAggregatorTest {

    private static PlanStatsAggregator aggregator;
    private static List<MktSnapshotsStatsRecord> records;

    private static long CONTEXT_ID = 6666666;
    private static String PREFIX = StringConstants.STAT_PREFIX_CURRENT;
    private static double CPU_CAPACITY = 111.111;
    private static double CPU_MIN = 10;
    private static double CPU_MID = 20;
    private static double CPU_MAX = 30;
    private static Collection<TopologyEntityDTO> leanDTOs;

    /**
     * Set up a topology of a few VMs and a few PMs.
     *
     */
    @BeforeClass
    public static void setup() {
        final TopologyEntityDTO vm1 = vm(10, EntityState.POWERED_ON);
        final TopologyEntityDTO vm2 = vm(20, EntityState.POWERED_ON);
        final TopologyEntityDTO suspendedVm = vm(25, EntityState.SUSPENDED);
        final TopologyEntityDTO pm1 = pm(30, CPU_MIN, EntityState.POWERED_ON);
        final TopologyEntityDTO pm2 = pm(40, CPU_MAX, EntityState.POWERED_ON);
        final TopologyEntityDTO pm3 = pm(50, CPU_MID, EntityState.POWERED_ON);
        final TopologyEntityDTO suspendedPm = pm(60, CPU_MAX, EntityState.SUSPENDED);
        final TopologyEntityDTO containerPod1 = containerPod(60);

        final TopologyInfo topologyOrganizer = TopologyInfo.newBuilder()
            .setTopologyContextId(CONTEXT_ID)
            .setTopologyId(200)
            .build();
        HistorydbIO historydbIO = new HistorydbIO(Mockito.mock(DBPasswordUtil.class), null);
        aggregator = new PlanStatsAggregator(null, historydbIO, topologyOrganizer, true);
        aggregator.handleChunk(Lists.newArrayList(vm1, pm1));
        aggregator.handleChunk(Lists.newArrayList(vm2, pm2, suspendedVm));
        aggregator.handleChunk(Lists.newArrayList(pm3, containerPod1, suspendedPm));
        records = aggregator.statsRecords();
    }

    /**
     * Verify that the aggregator reports the correct counts of entity types and
     * commodity sold types.
     *
     */
    @Test
    public void testCounts() {
        Map<Integer, Integer> entityTypeCounts = aggregator.getEntityTypeCounts();
        // The suspended VM should not be counted
        Assert.assertEquals(2, (int)entityTypeCounts.get(EntityType.VIRTUAL_MACHINE_VALUE));
        Assert.assertEquals(3, (int)entityTypeCounts.get(EntityType.PHYSICAL_MACHINE_VALUE));
        Assert.assertEquals(1, (int)entityTypeCounts.get(EntityType.CONTAINER_POD_VALUE));
        Assert.assertEquals(3, (int)aggregator.getCommodityTypeCounts().get(CommodityDTO.CommodityType.CPU_VALUE));
    }

    /**
     * Verify that the expected records are created: one record for CPU stats and four
     * more records for various topology stats.
     *
     */
    @Test
    public void testNumRecords() {
        List<String> propertyTypes = records.stream()
                .map(MktSnapshotsStatsRecord::getPropertyType)
                .collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList(
            PREFIX + "NumHosts", PREFIX + "NumVMsPerHost", PREFIX + "NumContainersPerHost",
            PREFIX + "NumVMs", PREFIX + "NumStorages", PREFIX + "NumVMsPerStorage",
            PREFIX + "NumContainersPerStorage", PREFIX + "NumContainers",
            PREFIX + "NumContainerPods", PREFIX + "NumCPUs", PREFIX + "CPU" ),
            propertyTypes);
    }

    /**
     * Verify that the CPU stats record is as expected.
     */
    @Test
    public void testCPURecord() {
        MktSnapshotsStatsRecord cpuStats = records.stream().filter(rec -> rec.getPropertyType()
            .contains("CPU") && !rec.getPropertyType()
                .contains("NumCPUs")).findFirst().get();
        Assert.assertEquals(CONTEXT_ID, (long)cpuStats.getMktSnapshotId());
        Assert.assertEquals(CPU_CAPACITY * 3, cpuStats.getCapacity(), 0);
        Assert.assertEquals((CPU_MIN + CPU_MID + CPU_MAX) / 3, cpuStats.getAvgValue(), 0);
        Assert.assertEquals(CPU_MIN, cpuStats.getMinValue(), 0);
        Assert.assertEquals(CPU_MAX, cpuStats.getMaxValue(), 0);
        Assert.assertEquals(PropertySubType.Used.getApiParameterName(),  cpuStats.getPropertySubtype());
        Assert.assertEquals(PREFIX + "CPU", cpuStats.getPropertyType());
    }

    /**
     * Verify that the numCPUs stats record is as expected.
     */
    @Test
    public void testNumCPUsRecord() {
        MktSnapshotsStatsRecord numCPUsStats = records.stream().filter(rec -> rec.getPropertyType()
                .contains("NumCPUs")).findFirst().get();

        Assert.assertEquals(PREFIX + "NumCPUs", numCPUsStats.getPropertyType());
        Assert.assertTrue(18 == numCPUsStats.getMaxValue());
        Assert.assertTrue(18 == numCPUsStats.getMinValue());
        Assert.assertTrue(18 == numCPUsStats.getAvgValue());
    }

    /**
     * Verify that the PM stats record is as expected.
     */
    @Test
    public void testNumPMsRecord() {
        MktSnapshotsStatsRecord pmsStats = records.stream().filter(rec -> rec.getPropertyType()
            .contains("NumHosts")).findFirst().get();
        Assert.assertEquals(CONTEXT_ID, (long)pmsStats.getMktSnapshotId());
        Assert.assertNull(pmsStats.getCapacity());
        Assert.assertEquals(3, pmsStats.getAvgValue(), 0);
        Assert.assertEquals(3, pmsStats.getMinValue(), 0);
        Assert.assertEquals(3, pmsStats.getMaxValue(), 0);
        Assert.assertNull(pmsStats.getPropertySubtype());
        Assert.assertEquals(PREFIX + "NumHosts", pmsStats.getPropertyType());
    }

    private static CommodityType CPU_TYPE
        = CommodityType.newBuilder().setType(CommodityDTO.CommodityType.CPU_VALUE).build();

    private static CommodityBoughtDTO cpuBought = CommodityBoughtDTO.newBuilder()
                    .setCommodityType(CPU_TYPE)
                    .build();

    private static TopologyEntityDTO vm(long oid, EntityState state) {
        return TopologyEntityDTO.newBuilder()
                    .setOid(oid)
                    .setDisplayName("VM-" + oid)
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .setEntityState(state)
                    // 999 is the provider id. Don't care that it doesn't exist.
                    .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(999)
                        .addCommodityBought(cpuBought))
                    .build();
    }

    private static TopologyEntityDTO pm(long oid, double cpuUsed, EntityState state) {
        return TopologyEntityDTO.newBuilder()
                    .setOid(oid)
                    .setDisplayName("PM-" + oid)
                    .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .setEntityState(state)
                    .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setPhysicalMachine(PhysicalMachineInfo.newBuilder().setNumCpus(6)))
                    .addCommoditySoldList(cpu(cpuUsed)).build();
    }

    private static CommoditySoldDTO cpu(double used) {
        return CommoditySoldDTO.newBuilder()
                    .setCommodityType(CPU_TYPE)
                    .setUsed(used)
                    .setCapacity(CPU_CAPACITY).build();
    }

    /**
     * Creates a containerPod TopologyEntityDTO.
     *
     * @param oid oid to use for the TopologyEntityDTO
     * @return TopologyEntityDTO created
     */
    private static TopologyEntityDTO containerPod(long oid) {
        return TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setDisplayName("ContainerPod-" + oid)
                .setEntityType(EntityType.CONTAINER_POD_VALUE)
                .build();
    }
}
