package com.vmturbo.repository.service;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.StatEpoch;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.service.PlanEntityStatsExtractor.DefaultPlanEntityStatsExtractor;

/**
 *  Test plan entity stats extraction from commodities.
 */
@RunWith(MockitoJUnitRunner.class)
public class PlanEntityStatsExtractorTest {

    /**
     * The class under test.
     */
    private final PlanEntityStatsExtractor statsExtractor = new DefaultPlanEntityStatsExtractor();

    /**
     * Test the process to extract stats for a single entity.
     */
    @Test
    public void testExtractStats() {
        // Prepare

        final double cpuUsedValue = 20;
        final double cpuPeakValue = 30;

        CommoditiesBoughtFromProvider commoditiesBought = CommoditiesBoughtFromProvider.newBuilder()
            .setProviderId(888)
            .addCommodityBought(CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.CPU_VALUE))
                .setUsed(cpuUsedValue)
                .setPeak(cpuPeakValue))
            .addCommodityBought(CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.MEM_VALUE))
                .setUsed(1024))
            .build();

        CommoditySoldDTO cpuCommoditySold =
        CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.VCPU_VALUE))
            .setUsed(cpuUsedValue)
            .setPeak(cpuPeakValue)
            .setCapacity(100)
            .build();

        final double vMemUsedValue = 1024;
        final double vMemCapacity = 8196;

        CommoditySoldDTO memCommoditySold =
            CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VMEM_VALUE))
                .setUsed(vMemUsedValue)
                .setCapacity(vMemCapacity)
                .build();

        ProjectedTopologyEntity entity = ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                .setOid(767676)
                .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
                .setDisplayName("SomeBusyVM")
                .setEntityState(EntityState.POWERED_ON)
                .addCommoditiesBoughtFromProviders(commoditiesBought)
                .addCommoditySoldList(cpuCommoditySold)
                .addCommoditySoldList(memCommoditySold))
            .setProjectedPriceIndex(500D)
            .build();

        StatsFilter statsFilter = StatsFilter.newBuilder()
            .build();

        StatEpoch statEpoch = StatEpoch.PLAN_PROJECTED;

        long snapshotDate = 100100100;

        // Apply
        final EntityStats stats =
            statsExtractor.extractStats(entity, statsFilter, statEpoch, snapshotDate)
                .build();

        // Verify
        assertEquals(1, stats.getStatSnapshotsCount());
        final StatSnapshot statSnapshot = stats.getStatSnapshotsList().iterator().next();
        assertEquals(statEpoch, statSnapshot.getStatEpoch());
        assertEquals(snapshotDate, statSnapshot.getSnapshotDate());
        // Check CPU stat
        final List<StatRecord> cpuStats = statSnapshot.getStatRecordsList().stream()
            .filter(StatRecord::hasName)
            .filter(statRecord -> statRecord.getName().equalsIgnoreCase(CommodityDTO.CommodityType.CPU.toString()))
            .collect(Collectors.toList());
        assertEquals(1, cpuStats.size());
        StatRecord cpuStat = cpuStats.iterator().next();
        assertEquals(cpuUsedValue, cpuStat.getCurrentValue(), 0);
        assertEquals(cpuUsedValue, cpuStat.getUsed().getAvg(), 0);
        assertEquals(cpuPeakValue, cpuStat.getUsed().getMax(), 0);
        assertEquals(cpuPeakValue, cpuStat.getUsed().getTotalMax(), 0);
        assertEquals(StringConstants.RELATION_BOUGHT, cpuStat.getRelation());
        // Check VMem stat
        final List<StatRecord> vMemStats = statSnapshot.getStatRecordsList().stream()
            .filter(StatRecord::hasName)
            .filter(statRecord -> statRecord.getName().equalsIgnoreCase(CommodityDTO.CommodityType.VMEM.toString()))
            .collect(Collectors.toList());
        assertEquals(1, vMemStats.size());
        StatRecord vMemStat = vMemStats.iterator().next();
        assertEquals(vMemUsedValue, vMemStat.getCurrentValue(), 0);
        assertEquals(vMemUsedValue, vMemStat.getUsed().getTotal(), 0);
        assertEquals(vMemCapacity, vMemStat.getCapacity().getTotal(), 0);
        assertEquals(StringConstants.RELATION_SOLD, vMemStat.getRelation());
    }
}
