package com.vmturbo.history.stats;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.schema.abstraction.tables.records.MktSnapshotsStatsRecord;
import com.vmturbo.history.utils.TopologyOrganizer;
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
    private static String PREFIX = "current";
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
        final TopologyEntityDTO vm1 = vm(10);
        final TopologyEntityDTO vm2 = vm(20);
        final TopologyEntityDTO pm1 = pm(30, CPU_MIN);
        final TopologyEntityDTO pm2 = pm(40, CPU_MAX);
        final TopologyEntityDTO pm3 = pm(50, CPU_MID);

        TopologyOrganizer topologyOrganizer = new TopologyOrganizer(CONTEXT_ID, 200);
        HistorydbIO historydbIO = Mockito.mock(HistorydbIO.class);
        aggregator = new PlanStatsAggregator(historydbIO, topologyOrganizer, true);
        aggregator.handleChunk(Lists.newArrayList(vm1, pm1));
        aggregator.handleChunk(Lists.newArrayList(vm2, pm2));
        aggregator.handleChunk(Lists.newArrayList(pm3));
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
        Assert.assertEquals(2, (int)entityTypeCounts.get(EntityType.VIRTUAL_MACHINE_VALUE));
        Assert.assertEquals(3, (int)entityTypeCounts.get(EntityType.PHYSICAL_MACHINE_VALUE));
        Assert.assertEquals(3, (int)aggregator.getCommodityTypeCounts().get(CommodityDTO.CommodityType.CPU_VALUE));
    }

    /**
     * Verify that the expected records are created: one record for CPU stats and four
     * more records for various topology stats.
     *
     */
    @Test
    public void testNumRecords() {
        Set<String> propertyTypes = records.stream()
                .map(MktSnapshotsStatsRecord::getPropertyType)
                .collect(Collectors.toSet());
        Assert.assertEquals(Sets.newHashSet(
            PREFIX + "CPU", PREFIX + "NumHosts", PREFIX + "NumVMs",
            PREFIX + "NumVMsPerHost", PREFIX + "NumContainersPerHost"), propertyTypes);
    }

    /**
     * Verify that the CPU stats record is as expected.
     *
     */
    @Test
    public void testCPURecord() {
        MktSnapshotsStatsRecord cpuStats = records.stream().filter(rec -> rec.getPropertyType()
            .contains("CPU")).findFirst().get();
        Assert.assertEquals(CONTEXT_ID, (long)cpuStats.getMktSnapshotId());
        Assert.assertEquals(CPU_CAPACITY, cpuStats.getCapacity(), 0);
        Assert.assertEquals((CPU_MIN + CPU_MID + CPU_MAX) / 3, cpuStats.getAvgValue(), 0);
        Assert.assertEquals(CPU_MIN, cpuStats.getMinValue(), 0);
        Assert.assertEquals(CPU_MAX, cpuStats.getMaxValue(), 0);
        Assert.assertEquals("used",  cpuStats.getPropertySubtype());
        Assert.assertEquals(PREFIX + "CPU", cpuStats.getPropertyType());
    }

    /**
     * Verify that the PM stats record is as expected.
     *
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

    private static TopologyEntityDTO vm(long oid) {
        return TopologyEntityDTO.newBuilder()
                    .setOid(oid)
                    .setDisplayName("VM-" + oid)
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    // 999 is the provider id. Don't care that it doesn't exist.
                    .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(999)
                        .addCommodityBought(cpuBought))
                    .build();
    }

    private static TopologyEntityDTO pm(long oid, double cpuUsed) {
        return TopologyEntityDTO.newBuilder()
                    .setOid(oid)
                    .setDisplayName("PM-" + oid)
                    .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .addCommoditySoldList(cpu(cpuUsed)).build();
    }

    private static CommoditySoldDTO cpu(double used) {
        return CommoditySoldDTO.newBuilder()
                    .setCommodityType(CPU_TYPE)
                    .setUsed(used)
                    .setCapacity(CPU_CAPACITY).build();
    }
}
