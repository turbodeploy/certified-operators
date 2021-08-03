package com.vmturbo.repository.service;

import static com.vmturbo.common.protobuf.utils.StringConstants.VCPU;
import static com.vmturbo.common.protobuf.utils.StringConstants.VCPU_OVERCOMMITMENT;
import static com.vmturbo.common.protobuf.utils.StringConstants.VMEM_OVERCOMMITMENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.StatEpoch;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.HistUtilizationValue;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ContainerPlatformClusterInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.service.AbridgedSoldCommoditiesForProvider.AbridgedSoldCommodity;
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

    private static final long SNAPSHOT_DATE = 100100100;
    private static final double DELTA = 0.0001;

    /**
     * Test the process to extract stats for a single entity.
     */
    @Test
    public void testExtractStatsForPlanProjectedEntity() {
        // Prepare

        final double cpuUsedValue = 20;
        final double cpuPeakValue = 30;
        final double vcpuPercentileValue = 50;
        final double vcpuCapacity = 100;
        final double iopsPercentile = 0.5;
        final double iopsCapacity = 4000;
        final long computeTierId = 888;

        CommoditiesBoughtFromProvider commoditiesBought = CommoditiesBoughtFromProvider.newBuilder()
            .setProviderId(computeTierId)
            .addCommodityBought(CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.CPU_VALUE))
                .setUsed(cpuUsedValue)
                .setPeak(cpuPeakValue))
            .addCommodityBought(CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.MEM_VALUE))
                .setUsed(1024))
            .addCommodityBought(CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE)
                    .build())
                .setHistoricalUsed(HistoricalValues.newBuilder()
                    .setPercentile(iopsPercentile)
                    .build())
                .setUsed(1300)
                .build())
            .build();

        CommoditySoldDTO cpuCommoditySold =
        CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.VCPU_VALUE))
            .setUsed(cpuUsedValue)
            .setPeak(cpuPeakValue)
            .setHistoricalUsed(HistoricalValues.newBuilder()
                    .setPercentile(vcpuPercentileValue / vcpuCapacity)
                    .setHistUtilization(vcpuPercentileValue)
                    .build())
            .setCapacity(vcpuCapacity)
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

        final Map<Long, AbridgedSoldCommoditiesForProvider> providerToSoldCommoditiesMap =
            Collections.singletonMap(computeTierId,
                new AbridgedSoldCommoditiesForProvider(EntityType.COMPUTE_TIER_VALUE,
                    Collections.singletonList(
                        new AbridgedSoldCommodity(
                            CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE, iopsCapacity))));
        // Apply
        final EntityStats stats =
            statsExtractor.extractStats(entity, statEpoch,
                providerToSoldCommoditiesMap, Collections.emptyMap(), Collections.emptyMap(),
                    SNAPSHOT_DATE)
                .build();

        // Verify
        assertEquals(1, stats.getStatSnapshotsCount());
        final StatSnapshot statSnapshot = stats.getStatSnapshotsList().iterator().next();
        assertEquals(statEpoch, statSnapshot.getStatEpoch());
        assertEquals(SNAPSHOT_DATE, statSnapshot.getSnapshotDate());
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

        final List<StatRecord> iopsStats = statSnapshot.getStatRecordsList().stream()
            .filter(StatRecord::hasName)
            .filter(statRecord -> statRecord.getName()
                .equalsIgnoreCase("StorageAccess"))
            .collect(Collectors.toList());
        Assert.assertFalse(iopsStats.isEmpty());
        final StatRecord iopsStatRecord = iopsStats.iterator().next();
        Assert.assertEquals(iopsCapacity, iopsStatRecord.getCapacity().getAvg(), 0);
        Assert.assertFalse(iopsStatRecord.getHistUtilizationValueList().isEmpty());
        final HistUtilizationValue histUtilizationValue =
            iopsStatRecord.getHistUtilizationValueList().iterator().next();
        Assert.assertEquals("percentile", histUtilizationValue.getType());
        Assert.assertEquals(iopsPercentile * iopsCapacity,
            histUtilizationValue.getUsage().getAvg(), 0);

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
        // Check VMem sold stat for "smoothed" values
        // PLAN_PROJECTED stat will have "smoothed" usage which is same as used value
        assertEquals(1, vMemStat.getHistUtilizationValueList().size());
        assertEquals("smoothed", vMemStat.getHistUtilizationValueList().get(0).getType());
        assertEquals(vMemUsedValue, vMemStat.getHistUtilizationValueList().get(0).getUsage().getAvg(), 0);
        assertEquals(vMemCapacity, vMemStat.getHistUtilizationValueList().get(0).getCapacity().getAvg(), 0);

        // Check VCPU sold stat for percentile values
        final List<StatRecord> vCpuStats = statSnapshot.getStatRecordsList().stream()
                .filter(StatRecord::hasName)
                .filter(statRecord -> statRecord.getName().equalsIgnoreCase(CommodityDTO.CommodityType.VCPU.toString()))
                .collect(Collectors.toList());
        assertEquals(1, vCpuStats.size());
        StatRecord vCpuStat = vCpuStats.iterator().next();
        assertEquals(2, vCpuStat.getHistUtilizationValueList().size());
        assertEquals("percentile", vCpuStat.getHistUtilizationValueList().get(0).getType());
        assertEquals(vcpuCapacity, vCpuStat.getHistUtilizationValueList().get(0).getCapacity().getAvg(), 0);
        assertEquals(vcpuPercentileValue, vCpuStat.getHistUtilizationValueList().get(0).getUsage().getAvg(), 0);
        // Check VCPU sold stat for "smoothed" values
        // "smoothed" usage is histUtilization.
        assertEquals("smoothed", vCpuStat.getHistUtilizationValueList().get(1).getType());
        assertEquals(vcpuCapacity, vCpuStat.getHistUtilizationValueList().get(1).getCapacity().getAvg(), 0);
        assertEquals(vcpuPercentileValue, vCpuStat.getHistUtilizationValueList().get(1).getUsage().getAvg(), 0);
    }

    /**
     * Test the process to extract stats for PLAN_SOURCE entity.
     */
    @Test
    public void testExtractStatsForPlanSourceEntity() {
        final double cpuUsedValue = 20;
        final double cpuPeakValue = 30;
        final double vcpuPercentileValue = 50;
        final double vcpuCapacity = 100;

        CommoditySoldDTO cpuCommoditySold =
            CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VCPU_VALUE))
                .setUsed(cpuUsedValue)
                .setPeak(cpuPeakValue)
                .setHistoricalUsed(HistoricalValues.newBuilder()
                    .setPercentile(vcpuPercentileValue / vcpuCapacity)
                    .setHistUtilization(vcpuPercentileValue)
                    .build())
                .setCapacity(vcpuCapacity)
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
                .addCommoditySoldList(cpuCommoditySold)
                .addCommoditySoldList(memCommoditySold))
            .setProjectedPriceIndex(500D)
            .build();

        StatEpoch statEpoch = StatEpoch.PLAN_SOURCE;
        final EntityStats stats =
            statsExtractor.extractStats(entity, statEpoch,
                new HashMap<>(), Collections.emptyMap(), Collections.emptyMap(),
                SNAPSHOT_DATE)
                .build();

        assertEquals(1, stats.getStatSnapshotsCount());
        final StatSnapshot statSnapshot = stats.getStatSnapshotsList().iterator().next();
        assertEquals(statEpoch, statSnapshot.getStatEpoch());
        assertEquals(SNAPSHOT_DATE, statSnapshot.getSnapshotDate());

        // Check VCPU stats
        final List<StatRecord> vCPUStats = statSnapshot.getStatRecordsList().stream()
            .filter(StatRecord::hasName)
            .filter(statRecord -> statRecord.getName().equalsIgnoreCase(CommodityDTO.CommodityType.VCPU.toString()))
            .collect(Collectors.toList());
        assertEquals(1, vCPUStats.size());
        StatRecord vCpuStat = vCPUStats.iterator().next();
        // Check VCPU sold stat for "percentile" values
        assertEquals(2, vCpuStat.getHistUtilizationValueList().size());
        assertEquals("percentile", vCpuStat.getHistUtilizationValueList().get(0).getType());
        assertEquals(vcpuCapacity, vCpuStat.getHistUtilizationValueList().get(0).getCapacity().getAvg(), 0);
        assertEquals(vcpuPercentileValue, vCpuStat.getHistUtilizationValueList().get(0).getUsage().getAvg(), 0);
        // Check VCPU sold stat for "smoothed" values
        assertEquals("smoothed", vCpuStat.getHistUtilizationValueList().get(1).getType());
        assertEquals(vcpuCapacity, vCpuStat.getHistUtilizationValueList().get(1).getCapacity().getAvg(), 0);
        assertEquals(vcpuPercentileValue, vCpuStat.getHistUtilizationValueList().get(1).getUsage().getAvg(), 0);

        // Check VMem stats
        final List<StatRecord> vMemStats = statSnapshot.getStatRecordsList().stream()
            .filter(StatRecord::hasName)
            .filter(statRecord -> statRecord.getName().equalsIgnoreCase(CommodityDTO.CommodityType.VMEM.toString()))
            .collect(Collectors.toList());
        assertEquals(1, vMemStats.size());
        StatRecord vMemStat = vMemStats.iterator().next();
        // No histUtilizationValue available because PLAN_SOURCE entity doesn't have historicalUsed
        // set up for VMem
        assertEquals(0, vMemStat.getHistUtilizationValueList().size());
    }

    /**
     * Test {@link DefaultPlanEntityStatsExtractor#extractStats} based on given commodity/attribute names.
     */
    @Test
    public void testExtractEntityAttributeStatsRecords() {
        final CommoditySoldDTO vcpuCommoditySold =
            CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VCPU_VALUE))
                .build();

        final double containerClusterVcpuOvercommitment = 1.5;
        final double containerClusterVmemOvercommitment = 0.4;
        final ContainerPlatformClusterInfo.Builder containerPlatformClusterInfo =
                ContainerPlatformClusterInfo.newBuilder()
                        .setVcpuOvercommitment(containerClusterVcpuOvercommitment)
                        .setVmemOvercommitment(containerClusterVmemOvercommitment);
        final ProjectedTopologyEntity containerClusterEntity = ProjectedTopologyEntity.newBuilder()
                .setEntity(TopologyEntityDTO.newBuilder()
                        .setOid(1234)
                        .setEntityType(EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE)
                        .setTypeSpecificInfo(
                                TypeSpecificInfo.newBuilder()
                                        .setContainerPlatformCluster(containerPlatformClusterInfo))
                        .addCommoditySoldList(vcpuCommoditySold))
                .build();
        final EntityStats containerClusterStats =
                statsExtractor.extractStats(containerClusterEntity, StatEpoch.PLAN_SOURCE,
                        Collections.emptyMap(), ImmutableMap.of(VCPU_OVERCOMMITMENT, new HashSet<>(),
                        VMEM_OVERCOMMITMENT, new HashSet<>(), VCPU, new HashSet<>()), Collections.emptyMap(),
                        SNAPSHOT_DATE).build();
        assertEquals(1, containerClusterStats.getStatSnapshotsCount());

        StatSnapshot statsSnapshots = containerClusterStats.getStatSnapshots(0);
        assertEquals(3, statsSnapshots.getStatRecordsCount());

        Map<String, StatRecord> statRecordMap = statsSnapshots.getStatRecordsList().stream()
                .collect(Collectors.toMap(StatRecord::getName, Function.identity()));
        assertTrue(statRecordMap.containsKey(VCPU_OVERCOMMITMENT));
        assertEquals(containerClusterVcpuOvercommitment, statRecordMap.get(VCPU_OVERCOMMITMENT).getCurrentValue(), DELTA);
        assertTrue(statRecordMap.containsKey(VMEM_OVERCOMMITMENT));
        assertEquals(containerClusterVmemOvercommitment, statRecordMap.get(VMEM_OVERCOMMITMENT).getCurrentValue(), DELTA);
        assertTrue(statRecordMap.containsKey(VCPU));
        assertEquals("mCores", statRecordMap.get(VCPU).getUnits());
    }
}
