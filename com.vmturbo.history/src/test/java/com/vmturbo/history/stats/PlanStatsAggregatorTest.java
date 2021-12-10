package com.vmturbo.history.stats;

import static com.vmturbo.common.protobuf.utils.StringConstants.STAT_PREFIX_CURRENT;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ContainerPlatformClusterInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
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

    private static final long CONTEXT_ID = 6666666;
    private static final String PREFIX = STAT_PREFIX_CURRENT;
    private static final double CPU_CAPACITY = 111.111;
    private static final double CPU_MIN = 10;
    private static final double CPU_MID = 20;
    private static final double CPU_MAX = 30;
    private static final double VCPU_OVERCOMMITMENT_VAL = 0.5;
    private static final double VMEM_OVERCOMMITMENT_VAL = 1.2;
    private static final long DEFAULT_PROVIDER_ID = 999;
    private static final double DELTA = 0.01;

    private static final String OVERCOMMITMENT = "Overcommitment";
    private static final String CUR_VCPU_OVERCOMMITMENT = "currentVCPUOvercommitment";
    private static final String CUR_VMEM_OVERCOMMITMENT = "currentVMemOvercommitment";

    /**
     * Set up a topology of a few VMs and a few PMs.
     *
     */
    @BeforeClass
    public static void setup() {
        final TopologyEntityDTO vm1 = vm(10, EntityState.POWERED_ON);
        final TopologyEntityDTO vm2 = vm(20, EntityState.POWERED_ON);
        // Suspended VM -- should be filtered out
        final TopologyEntityDTO suspendedVm = vm(25, EntityState.SUSPENDED);
        // Unplaced VM -- should be filtered out
        final TopologyEntityDTO unplacedVm = vm(26, EntityState.POWERED_ON, -1);
        final TopologyEntityDTO pm1 = pm(30, CPU_MIN, EntityState.POWERED_ON);
        final TopologyEntityDTO pm2 = pm(40, CPU_MAX, EntityState.POWERED_ON);
        final TopologyEntityDTO pm3 = pm(50, CPU_MID, EntityState.POWERED_ON, -1);
        // Suspended Host -- should be filtered out
        final TopologyEntityDTO suspendedPm = pm(60, CPU_MAX, EntityState.SUSPENDED);
        final TopologyEntityDTO containerPod1 = containerPod(60);
        final TopologyEntityDTO diskArray = diskArray(70, 500, 2000);
        final TopologyEntityDTO storage1 = storage(80, 200, 1000);
        final TopologyEntityDTO storage2 = storage(81, 300, 1000);

        final TopologyEntityDTO containerCluster = containerPlatformCluster(90);

        final TopologyInfo topologyOrganizer = TopologyInfo.newBuilder()
            .setTopologyContextId(CONTEXT_ID)
            .setTopologyId(200)
            .build();
        HistorydbIO historydbIO = new HistorydbIO(null, null);
        aggregator = new PlanStatsAggregator(null, historydbIO, topologyOrganizer, true);
        aggregator.handleChunk(Lists.newArrayList(vm1, pm1, unplacedVm));
        aggregator.handleChunk(Lists.newArrayList(vm2, pm2, suspendedVm));
        aggregator.handleChunk(Lists.newArrayList(pm3, containerPod1, suspendedPm));
        aggregator.handleChunk(Lists.newArrayList(diskArray, storage1, storage2));
        aggregator.handleChunk(Lists.newArrayList(containerCluster));
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
        assertThat(propertyTypes, containsInAnyOrder(
            PREFIX + "NumHosts", PREFIX + "NumVMsPerHost", PREFIX + "NumContainersPerHost",
            PREFIX + "NumVMs", PREFIX + "NumStorages", PREFIX + "NumVMsPerStorage",
            PREFIX + "NumContainersPerStorage", PREFIX + "NumContainers",
            PREFIX + "NumContainerPods", PREFIX + "NumCPUs", PREFIX + "CPU",
            // "currentStorageAmount" should appear twice: once with relatedEntityType storage
            // and once with relatedEntityType disk array.
            PREFIX + "StorageAmount", PREFIX + "StorageAmount",
            PREFIX + "VCPUOvercommitment", PREFIX + "VMemOvercommitment"));
    }

    /**
     * Verify that the CPU stats record is as expected.
     */
    @Test
    public void testCPURecord() {
        MktSnapshotsStatsRecord cpuStats = records.stream().filter(rec -> rec.getPropertyType()
            .contains("CPU") && !rec.getPropertyType()
                .contains("NumCPUs")
            && !rec.getPropertyType().contains(OVERCOMMITMENT)).findFirst().get();
        Assert.assertEquals(CONTEXT_ID, (long)cpuStats.getMktSnapshotId());
        Assert.assertEquals(CPU_CAPACITY * 3, cpuStats.getCapacity(), 0);
        Assert.assertEquals((CPU_MIN + CPU_MID + CPU_MAX) / 3, cpuStats.getAvgValue(), 0);
        Assert.assertEquals(CPU_MIN, cpuStats.getMinValue(), 0);
        Assert.assertEquals(CPU_MAX, cpuStats.getMaxValue(), 0);
        Assert.assertEquals(PropertySubType.Used.getApiParameterName(),  cpuStats.getPropertySubtype());
        Assert.assertEquals(PREFIX + "CPU", cpuStats.getPropertyType());
    }

    /**
     * Verify that the Storage stats record is as expected.
     *
     * <p>Specifically, ensure that the Storage amount (used and capacity) is not double-counted as
     * a result of the presence of both a DiskArray and a Storage device.</p>
     */
    @Test
    public void testStorageRecord() {
        Collection<MktSnapshotsStatsRecord> storageStatsRecords = records.stream()
            .filter(rec -> rec.getPropertyType().contains("StorageAmount"))
            .filter(rec -> EntityType.STORAGE_VALUE == rec.getEntityType())
            .collect(Collectors.toCollection(ArrayList::new));
        Assert.assertEquals(1, storageStatsRecords.size());
        MktSnapshotsStatsRecord storageStatsRecord = storageStatsRecords.iterator().next();
        Assert.assertEquals(CONTEXT_ID, (long)storageStatsRecord.getMktSnapshotId());
        // The aggregated capacity of both storage devices (but not the DiskArray)
        Assert.assertEquals(2000, storageStatsRecord.getCapacity(), 0);
        // The average used value of both storage devices (but not the DiskArray)
        Assert.assertEquals(250, storageStatsRecord.getAvgValue(), 0);
        // The min used value of both storage devices (but not the DiskArray)
        Assert.assertEquals(200, storageStatsRecord.getMinValue(), 0);
        // The max used value of both storage devices (but not the DiskArray)
        Assert.assertEquals(300, storageStatsRecord.getMaxValue(), 0);
    }

    /**
     * Verify that the numCPUs stats record is as expected.
     */
    @Test
    public void testNumCPUsRecord() {
        MktSnapshotsStatsRecord numCPUsStats = records.stream().filter(rec -> rec.getPropertyType()
                .contains("NumCPUs")).findFirst().get();

        Assert.assertEquals(PREFIX + "NumCPUs", numCPUsStats.getPropertyType());
        Assert.assertEquals(18, numCPUsStats.getMaxValue(), 0.0);
        Assert.assertEquals(18, numCPUsStats.getMinValue(), 0.0);
        Assert.assertEquals(18, numCPUsStats.getAvgValue(), 0.0);
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

    /**
     * Test ContainerPlatformCluster vCPUOvercommitment and vMemOvercommitment records.
     */
    @Test
    public void testContainerClusterOvercommitmentsSourceRecord() {
        Map<String, MktSnapshotsStatsRecord> cntClusterOvercommitmentsRecord =
            records.stream().filter(rec -> rec.getPropertyType()
                .contains(OVERCOMMITMENT)).collect(Collectors.toMap(MktSnapshotsStatsRecord::getPropertyType, Function.identity()));
        Assert.assertEquals(2, cntClusterOvercommitmentsRecord.size());
        final MktSnapshotsStatsRecord vCPUOvercommitmentRecord = cntClusterOvercommitmentsRecord.get(CUR_VCPU_OVERCOMMITMENT);
        // VCPUOvercommitment record.
        Assert.assertEquals(CUR_VCPU_OVERCOMMITMENT, vCPUOvercommitmentRecord.getPropertyType());
        Assert.assertNull(vCPUOvercommitmentRecord.getPropertySubtype());
        Assert.assertNull(vCPUOvercommitmentRecord.getCapacity());
        Assert.assertEquals(VCPU_OVERCOMMITMENT_VAL, vCPUOvercommitmentRecord.getAvgValue(), DELTA);
        Assert.assertEquals(VCPU_OVERCOMMITMENT_VAL, vCPUOvercommitmentRecord.getMinValue(), DELTA);
        Assert.assertEquals(VCPU_OVERCOMMITMENT_VAL, vCPUOvercommitmentRecord.getMaxValue(), DELTA);
        // VMemOvercommitment record.
        final MktSnapshotsStatsRecord vMemOvercommitmentRecord = cntClusterOvercommitmentsRecord.get(CUR_VMEM_OVERCOMMITMENT);
        Assert.assertEquals(CUR_VMEM_OVERCOMMITMENT, vMemOvercommitmentRecord.getPropertyType());
        Assert.assertNull(vMemOvercommitmentRecord.getPropertySubtype());
        Assert.assertNull(vMemOvercommitmentRecord.getCapacity());
        Assert.assertEquals(VMEM_OVERCOMMITMENT_VAL, vMemOvercommitmentRecord.getAvgValue(), DELTA);
        Assert.assertEquals(VMEM_OVERCOMMITMENT_VAL, vMemOvercommitmentRecord.getMinValue(), DELTA);
        Assert.assertEquals(VMEM_OVERCOMMITMENT_VAL, vMemOvercommitmentRecord.getMaxValue(), DELTA);
    }

    private static final CommodityType CPU_TYPE
        = CommodityType.newBuilder().setType(CommodityDTO.CommodityType.CPU_VALUE).build();

    private static final CommodityType STORAGE_AMOUNT_TYPE
        = CommodityType.newBuilder().setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE).build();

    private static final CommodityBoughtDTO cpuBought = CommodityBoughtDTO.newBuilder()
                    .setCommodityType(CPU_TYPE)
                    .build();

    private static TopologyEntityDTO vm(long oid, EntityState state) {
        // Place VMs by default;
        return vm(oid, state, DEFAULT_PROVIDER_ID);
    }

    private static TopologyEntityDTO vm(long oid, EntityState state, long providerId) {
        return TopologyEntityDTO.newBuilder()
                    .setOid(oid)
                    .setDisplayName("VM-" + oid)
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .setEntityState(state)
                    // 999 is the provider id. Don't care that it doesn't exist.
                    .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(providerId)
                        .addCommodityBought(cpuBought))
                    .build();
    }

    private static TopologyEntityDTO pm(long oid, double cpuUsed, EntityState state) {
        // Place PMs by default
        return pm(oid, cpuUsed, state, DEFAULT_PROVIDER_ID);
    }

    private static TopologyEntityDTO pm(long oid, double cpuUsed, EntityState state, long providerId) {
        return TopologyEntityDTO.newBuilder()
            .setOid(oid)
            .setDisplayName("PM-" + oid)
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setEntityState(state)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setPhysicalMachine(PhysicalMachineInfo.newBuilder().setNumCpus(6)))
            .addCommoditySoldList(cpu(cpuUsed))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.DATACENTER_VALUE)))
                .setProviderId(providerId)
                .build())
            .build();
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

    /**
     * Creates a containerPlatformCluster TopologyEntityDTO.
     *
     * @param oid oid to use for the TopologyEntityDTO.
     * @return TopologyEntityDTO created.
     */
    private static TopologyEntityDTO containerPlatformCluster(long oid) {
        return TopologyEntityDTO.newBuilder()
            .setOid(oid)
            .setDisplayName("ContainerPlatformCluster-" + oid)
            .setEntityType(EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setContainerPlatformCluster(ContainerPlatformClusterInfo.newBuilder()
                    .setVcpuOvercommitment(VCPU_OVERCOMMITMENT_VAL)
                    .setVmemOvercommitment(VMEM_OVERCOMMITMENT_VAL)))
            .build();
    }

    /**
     * Creates a DiskArray TopologyEntityDTO.
     *
     * @param oid oid to use for the TopologyEntityDTO
     * @param storageUsed the storage amount used
     * @param storageCapacity the storage capacity
     * @return TopologyEntityDTO created
     */
    private static TopologyEntityDTO diskArray(long oid, double storageUsed, double storageCapacity) {
        return TopologyEntityDTO.newBuilder()
            .setOid(oid)
            .setDisplayName("DiskArray-" + oid)
            .setEntityType(EntityType.DISK_ARRAY_VALUE)
            .addCommoditySoldList(storageAmount(storageUsed, storageCapacity))
            .build();
    }

    /**
     * Creates a Storage TopologyEntityDTO.
     *
     * @param oid oid to use for the TopologyEntityDTO
     * @param storageUsed the storage amount used
     * @param storageCapacity the storage capacity
     * @return TopologyEntityDTO created
     */
    private static TopologyEntityDTO storage(long oid, double storageUsed, double storageCapacity) {
        return TopologyEntityDTO.newBuilder()
            .setOid(oid)
            .setDisplayName("Storage-" + oid)
            .setEntityType(EntityType.STORAGE_VALUE)
            .addCommoditySoldList(storageAmount(storageUsed, storageCapacity))
            .build();
    }

    /**
     * Creates a StorageAmount CommoditySoldDTO.
     *
     * @param storageUsed the used
     * @param storageCapacity the capacity
     * @return a StorageAmount CommoditySoldDTO
     */
    private static CommoditySoldDTO storageAmount(double storageUsed, double storageCapacity) {
        return CommoditySoldDTO.newBuilder()
            .setCommodityType(STORAGE_AMOUNT_TYPE)
            .setUsed(storageUsed)
            .setCapacity(storageCapacity)
            .build();
    }
}
