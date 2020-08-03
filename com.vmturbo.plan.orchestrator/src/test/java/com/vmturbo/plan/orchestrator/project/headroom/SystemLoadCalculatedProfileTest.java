package com.vmturbo.plan.orchestrator.project.headroom;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.vmturbo.common.protobuf.TemplateProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadRecord;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.plan.orchestrator.project.headroom.SystemLoadCalculatedProfile.Operation;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 *Class to test SystemLoadCalculatedProfile.
 */
public class SystemLoadCalculatedProfileTest {

    private double delta = 0.1;

    /**
     * Set commodities across two virtual machines for avg profile
     * and check for average values.
     */
    @Test
    public void testCreateVirtualMachineAvgProfile() {
        int clusterId = 1;
        List<SystemLoadRecord> systemLoadRecordList = Arrays.asList(
            // Records with VM id = 5 and cluster id 1
            createRecord(0, 5, clusterId, "VCPU", 100),
            createRecord(1, 5, clusterId, "CPU", 9),
            createRecord(0, 5, clusterId, "VMEM", 50),
            createRecord(1, 5, clusterId, "MEM", 10),
            createRecord(1, 5, clusterId, "STORAGE_AMOUNT", 9),
            createRecord(1, 5, clusterId, "NET_THROUGHPUT", 7),
            createRecord(1, 5, clusterId, "IO_THROUGHPUT", 8),
            createRecord(1, 5, clusterId, "STORAGE_ACCESS", 15),
            // Records with VM id = 6 and cluster id 1
            createRecord(0, 6, clusterId, "VMEM", 40),
            createRecord(1, 6, clusterId, "MEM", 20),
            createRecord(0, 6, clusterId, "VCPU", 50),
            createRecord(1, 6, clusterId, "CPU", 6),
            createRecord(1, 6, clusterId, "STORAGE_AMOUNT", 5),
            createRecord(1, 6, clusterId, "NET_THROUGHPUT", 21),
            createRecord(1, 6, clusterId, "IO_THROUGHPUT", 24),
            createRecord(1, 6, clusterId, "STORAGE_ACCESS", 25)
        );

        SystemLoadCalculatedProfile profile = getSystemLoadCalculatedProfile(
            Operation.AVG, getClusterWithId(clusterId), systemLoadRecordList);

        profile.createVirtualMachineProfile();

        assertTrue(profile.getHeadroomTemplateInfo().isPresent());
        TemplateInfo templateInfo = profile.getHeadroomTemplateInfo().get();
        assertEquals(templateInfo.getEntityType(),  EntityType.VIRTUAL_MACHINE_VALUE);

        // MEM avg = (50 + 40)/2
        Optional<TemplateField> mem = getField(TemplateProtoUtil.VM_COMPUTE_MEM_SIZE, templateInfo);
        assertTrue(mem.isPresent());
        assertEquals(45d, Double.valueOf(mem.get().getValue()), delta);

        // MEM consumption factor = (20 + 10)/(50 + 40)
        Optional<TemplateField> memConsumption = getField(TemplateProtoUtil.VM_COMPUTE_MEM_CONSUMED_FACTOR, templateInfo);
        assertTrue(memConsumption.isPresent());
        assertEquals(0.3333d, Double.valueOf(memConsumption.get().getValue()), delta);

        // CPU avg = (100 + 50)/2
        Optional<TemplateField> cpu = getField(TemplateProtoUtil.VM_COMPUTE_VCPU_SPEED, templateInfo);
        assertTrue(cpu.isPresent());
        assertEquals(75d, Double.valueOf(cpu.get().getValue()), delta);

        // MEM consumption factor = (9 + 6)/(100 + 50)
        Optional<TemplateField> cpuConsumption = getField(TemplateProtoUtil.VM_COMPUTE_CPU_CONSUMED_FACTOR, templateInfo);
        assertTrue(cpuConsumption.isPresent());
        assertEquals(0.1d, Double.valueOf(cpuConsumption.get().getValue()), delta);

        // Storage avg = (9 + 5)/2
        Optional<TemplateField> storage = getField(TemplateProtoUtil.VM_STORAGE_DISK_SIZE, templateInfo);
        assertTrue(storage.isPresent());
        assertEquals(7d, Double.valueOf(storage.get().getValue()), delta);

        // Storage consumption value is set to default value.
        Optional<TemplateField> diskConsumption = getField(TemplateProtoUtil.VM_STORAGE_DISK_CONSUMED_FACTOR, templateInfo);
        assertTrue(diskConsumption.isPresent());
        assertEquals(TemplateProtoUtil.VM_STORAGE_DISK_CONSUMED_FACTOR_DEFAULT_VALUE,
                        Double.valueOf(diskConsumption.get().getValue()), delta);

        // NET_THROUGHPUT avg = (21 + 7)/2
        Optional<TemplateField> netThroughput = getField(TemplateProtoUtil.VM_COMPUTE_NETWORK_THROUGHPUT, templateInfo);
        assertTrue(netThroughput.isPresent());
        assertEquals(14d, Double.valueOf(netThroughput.get().getValue()), delta);

        // IO_THROUGHPUT avg = (8 + 24)/2
        Optional<TemplateField> io = getField(TemplateProtoUtil.VM_COMPUTE_IO_THROUGHPUT, templateInfo);
        assertTrue(io.isPresent());
        assertEquals(16d, Double.valueOf(io.get().getValue()), delta);

        // STORAGE_ACCESS avg = (15 + 25)/2
        Optional<TemplateField> accessSpeed = getField(TemplateProtoUtil.VM_STORAGE_DISK_IOPS, templateInfo);
        assertTrue(accessSpeed.isPresent());
        assertEquals(20d, Double.valueOf(accessSpeed.get().getValue()), delta);
    }

    /**
     * Set commodities across two virtual machines for avg profile
     *  and check for max consumption factor as used > capacity.
     */
    @Test
    public void testCreateVirtualMachineAvgProfileMaxConsumptionFactors() {
        int clusterId = 1;
        List<SystemLoadRecord> systemLoadRecordList = Arrays.asList(
            // Records with VM id = 5 and cluster id 1
            createRecord(0, 5, clusterId, "VMEM", 50),
            createRecord(1, 5, clusterId, "MEM", 100),
            createRecord(0, 5, clusterId, "VCPU", 100),
            createRecord(1, 5, clusterId, "CPU", 200),
            // Records with VM id = 6 and cluster id 1
            createRecord(0, 6, clusterId, "VMEM", 40),
            createRecord(1, 6, clusterId, "MEM", 60),
            createRecord(0, 6, clusterId, "VCPU", 50),
            createRecord(1, 6, clusterId, "CPU", 60)
        );

        SystemLoadCalculatedProfile profile = getSystemLoadCalculatedProfile(
            Operation.AVG, getClusterWithId(clusterId), systemLoadRecordList);

        profile.createVirtualMachineProfile();

        assertTrue(profile.getHeadroomTemplateInfo().isPresent());
        TemplateInfo templateInfo = profile.getHeadroomTemplateInfo().get();
        assertEquals(templateInfo.getEntityType(),  EntityType.VIRTUAL_MACHINE_VALUE);

        // MEM consumption factor = (100 + 60) > (50 + 40) : Max consumption factor
        Optional<TemplateField> memConsumption = getField(TemplateProtoUtil.VM_COMPUTE_MEM_CONSUMED_FACTOR, templateInfo);
        assertTrue(memConsumption.isPresent());
        assertEquals(1d, Double.valueOf(memConsumption.get().getValue()), delta);

        // MEM consumption factor = (200 + 60) > (100 + 50) : Max consumption factor
        Optional<TemplateField> cpuConsumption = getField(TemplateProtoUtil.VM_COMPUTE_CPU_CONSUMED_FACTOR, templateInfo);
        assertTrue(cpuConsumption.isPresent());
        assertEquals(1d, Double.valueOf(cpuConsumption.get().getValue()), delta);
    }

    /**
     * Set commodities across two virtual machines for max profile and check for max values.
     */
    @Test
    public void testCreateVirtualMachineMaxProfile() {
        int clusterId = 1;
        List<SystemLoadRecord> systemLoadRecordList = Arrays.asList(
            // Records with VM id = 5 and cluster id 1
            createRecord(0, 5, clusterId, "VMEM", 50),
            createRecord(1, 5, clusterId, "MEM", 10),
            createRecord(0, 5, clusterId, "VCPU", 100),
            createRecord(1, 5, clusterId, "CPU", 9),
            createRecord(1, 5, clusterId, "STORAGE_AMOUNT", 9),
            createRecord(1, 5, clusterId, "NET_THROUGHPUT", 7),
            createRecord(1, 5, clusterId, "IO_THROUGHPUT", 8),
            createRecord(1, 5, clusterId, "STORAGE_ACCESS", 15),
            // Records with VM id = 6 and cluster id 1
            createRecord(0, 6, clusterId, "VMEM", 40),
            createRecord(1, 6, clusterId, "MEM", 20),
            createRecord(0, 6, clusterId, "VCPU", 50),
            createRecord(1, 6, clusterId, "CPU", 6),
            createRecord(1, 6, clusterId, "STORAGE_AMOUNT", 5),
            createRecord(1, 6, clusterId, "NET_THROUGHPUT", 21),
            createRecord(1, 6, clusterId, "IO_THROUGHPUT", 24),
            createRecord(1, 6, clusterId, "STORAGE_ACCESS", 25)
        );

        SystemLoadCalculatedProfile profile = getSystemLoadCalculatedProfile(
            Operation.MAX, getClusterWithId(clusterId), systemLoadRecordList);

        profile.createVirtualMachineProfile();

        assertTrue(profile.getHeadroomTemplateInfo().isPresent());
        TemplateInfo templateInfo = profile.getHeadroomTemplateInfo().get();
        assertEquals(templateInfo.getEntityType(),  EntityType.VIRTUAL_MACHINE_VALUE);

        // MEM : max(40,50)
        Optional<TemplateField> mem = getField(TemplateProtoUtil.VM_COMPUTE_MEM_SIZE, templateInfo);
        assertTrue(mem.isPresent());
        assertEquals(50d, Double.valueOf(mem.get().getValue()), delta);

        // MEM consumption factor = max(20,10)/max(50,40)
        Optional<TemplateField> memConsumption = getField(TemplateProtoUtil.VM_COMPUTE_MEM_CONSUMED_FACTOR, templateInfo);
        assertTrue(memConsumption.isPresent());
        assertEquals(0.4d, Double.valueOf(memConsumption.get().getValue()), delta);

        // CPU : max(100,50)
        Optional<TemplateField> cpu = getField(TemplateProtoUtil.VM_COMPUTE_VCPU_SPEED, templateInfo);
        assertTrue(cpu.isPresent());
        assertEquals(100d, Double.valueOf(cpu.get().getValue()), delta);

        // MEM consumption factor = max(9,6)/max(100,50)
        Optional<TemplateField> cpuConsumption = getField(TemplateProtoUtil.VM_COMPUTE_CPU_CONSUMED_FACTOR, templateInfo);
        assertTrue(cpuConsumption.isPresent());
        assertEquals(0.09d, Double.valueOf(cpuConsumption.get().getValue()), delta);

        // Storage : max(9,5)
        Optional<TemplateField> storage = getField(TemplateProtoUtil.VM_STORAGE_DISK_SIZE, templateInfo);
        assertTrue(storage.isPresent());
        assertEquals(9d, Double.valueOf(storage.get().getValue()), delta);

        // Storage consumption value is set to default value.
        Optional<TemplateField> diskConsumption = getField(TemplateProtoUtil.VM_STORAGE_DISK_CONSUMED_FACTOR, templateInfo);
        assertTrue(diskConsumption.isPresent());
        assertEquals(TemplateProtoUtil.VM_STORAGE_DISK_CONSUMED_FACTOR_DEFAULT_VALUE,
                        Double.valueOf(diskConsumption.get().getValue()), delta);

        // NET_THROUGHPUT : max(21,7)
        Optional<TemplateField> netThroughput = getField(TemplateProtoUtil.VM_COMPUTE_NETWORK_THROUGHPUT, templateInfo);
        assertTrue(netThroughput.isPresent());
        assertEquals(21, Double.valueOf(netThroughput.get().getValue()), delta);

        // IO_THROUGHPUT : max(8,24)
        Optional<TemplateField> io = getField(TemplateProtoUtil.VM_COMPUTE_IO_THROUGHPUT, templateInfo);
        assertTrue(io.isPresent());
        assertEquals(24d, Double.valueOf(io.get().getValue()), delta);

        // STORAGE_ACCESS : max(15, 25)
        Optional<TemplateField> accessSpeed = getField(TemplateProtoUtil.VM_STORAGE_DISK_IOPS, templateInfo);
        assertTrue(accessSpeed.isPresent());
        assertEquals(25d, Double.valueOf(accessSpeed.get().getValue()), delta);
    }

    /**
     * Set commodities across two virtual machines and check for max consumption factor as used > capacity.
     */
    @Test
    public void testCreateVirtualMachineMaxProfileMaxConsumptionFactors() {
        int clusterId = 1;
        List<SystemLoadRecord> systemLoadRecordList = Arrays.asList(
            // Records with VM id = 5 and cluster id 1
            createRecord(0, 5, clusterId, "VMEM", 50),
            createRecord(1, 5, clusterId, "MEM", 100),
            createRecord(0, 5, clusterId, "VCPU", 100),
            createRecord(1, 5, clusterId, "CPU", 200),
            // Records with VM id = 6 and cluster id 1
            createRecord(0, 6, clusterId, "VMEM", 40),
            createRecord(1, 6, clusterId, "MEM", 60),
            createRecord(0, 6, clusterId, "VCPU", 50),
            createRecord(1, 6, clusterId, "CPU", 60)
        );

        SystemLoadCalculatedProfile profile = getSystemLoadCalculatedProfile(
            Operation.MAX, getClusterWithId(clusterId), systemLoadRecordList);

        profile.createVirtualMachineProfile();

        assertTrue(profile.getHeadroomTemplateInfo().isPresent());
        TemplateInfo templateInfo = profile.getHeadroomTemplateInfo().get();
        assertEquals(templateInfo.getEntityType(),  EntityType.VIRTUAL_MACHINE_VALUE);

        // MEM consumption factor = (100 + 60) > (50 + 40) : Max consumption factor
        Optional<TemplateField> memConsumption = getField(TemplateProtoUtil.VM_COMPUTE_MEM_CONSUMED_FACTOR, templateInfo);
        assertTrue(memConsumption.isPresent());
        assertEquals(1d, Double.valueOf(memConsumption.get().getValue()), delta);

        // MEM consumption factor = (200 + 60) > (100 + 50) : Max consumption factor
        Optional<TemplateField> cpuConsumption = getField(TemplateProtoUtil.VM_COMPUTE_CPU_CONSUMED_FACTOR, templateInfo);
        assertTrue(cpuConsumption.isPresent());
        assertEquals(1d, Double.valueOf(cpuConsumption.get().getValue()), delta);
    }

    private Optional<TemplateField> getField(String name, TemplateInfo templateInfo) {
        return templateInfo.getResourcesList().stream()
        .flatMap(res -> res.getFieldsList().stream())
        .filter(f -> f.getName().equals(name))
        .findFirst();
    }

    private SystemLoadCalculatedProfile getSystemLoadCalculatedProfile(
            @Nonnull final Operation operation, @Nonnull final Grouping cluster,
            @Nonnull final List<SystemLoadRecord> systemLoadRecordList) {
        return new SystemLoadCalculatedProfile(operation, cluster, systemLoadRecordList,
                        cluster.getDefinition().getDisplayName(), "", Collections.emptyMap());
    }

    private Grouping getClusterWithId(long clusterId) {
        return Grouping.newBuilder()
            .setId(clusterId)
            .addExpectedTypes(MemberType.newBuilder().setEntity(ApiEntityType.PHYSICAL_MACHINE.typeNumber()))
            .setDefinition(GroupDefinition.newBuilder()
                            .setDisplayName("Cluster1")
                            .setType(GroupType.COMPUTE_HOST_CLUSTER))
            .build();
    }

    private SystemLoadRecord createRecord(int relationType, int vmId, int clusterId, String propertyType, double val) {
        return SystemLoadRecord.newBuilder()
            .setClusterId(clusterId)
            .setUuid(vmId)
            .setRelationType(relationType)
            .setPropertyType(propertyType)
            .setAvgValue(val)
            .setCapacity(val)
            .setPropertySubtype("used")
            .build();
    }
}
