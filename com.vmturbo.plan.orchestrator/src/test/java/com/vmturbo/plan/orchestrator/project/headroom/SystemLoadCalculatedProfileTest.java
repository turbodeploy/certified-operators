package com.vmturbo.plan.orchestrator.project.headroom;

import static org.junit.Assert.*;

import java.util.Optional;

import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadInfoResponse;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadRecord;
import com.vmturbo.plan.orchestrator.project.headroom.SystemLoadCalculatedProfile.Operation;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

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
        SystemLoadInfoResponse.Builder resp = SystemLoadInfoResponse.newBuilder();
        int clusterId = 1;
        // Records with VM id = 5 and cluster id 1
        resp.addRecord(createRecord(0, 5, clusterId, "VCPU", 100));
        resp.addRecord(createRecord(1, 5, clusterId, "CPU", 9));
        resp.addRecord(createRecord(0, 5, clusterId, "VMEM", 50));
        resp.addRecord(createRecord(1, 5, clusterId, "MEM", 10));
        resp.addRecord(createRecord(1, 5, clusterId, "STORAGE_AMOUNT", 9));
        resp.addRecord(createRecord(1, 5, clusterId, "NET_THROUGHPUT", 7));
        resp.addRecord(createRecord(1, 5, clusterId, "IO_THROUGHPUT", 8));
        resp.addRecord(createRecord(1, 5, clusterId, "STORAGE_ACCESS", 15));
        // Records with VM id = 6 and cluster id 1
        resp.addRecord(createRecord(0, 6, clusterId, "VMEM", 40));
        resp.addRecord(createRecord(1, 6, clusterId, "MEM", 20));
        resp.addRecord(createRecord(0, 6, clusterId, "VCPU", 50));
        resp.addRecord(createRecord(1, 6, clusterId, "CPU", 6));
        resp.addRecord(createRecord(1, 6, clusterId, "STORAGE_AMOUNT", 5));
        resp.addRecord(createRecord(1, 6, clusterId, "NET_THROUGHPUT", 21));
        resp.addRecord(createRecord(1, 6, clusterId, "IO_THROUGHPUT", 24));
        resp.addRecord(createRecord(1, 6, clusterId, "STORAGE_ACCESS", 25));
        SystemLoadCalculatedProfile profile =
                        getSystemLoadCalculatedProfile(Operation.AVG, getClusterWithId(clusterId), resp.build());

        profile.createVirtualMachineProfile();

        assertTrue(profile.getHeadroomTemplateInfo().isPresent());
        TemplateInfo templateInfo = profile.getHeadroomTemplateInfo().get();
        assertEquals(templateInfo.getEntityType(),  EntityType.VIRTUAL_MACHINE_VALUE);

        // MEM avg = (50 + 40)/2
        Optional<TemplateField> mem = getField(SystemLoadCalculatedProfile.MEMORY_SIZE, templateInfo);
        assertTrue(mem.isPresent());
        assertEquals(45d, Double.valueOf(mem.get().getValue()), delta);

        // MEM consumption factor = (20 + 10)/(50 + 40)
        Optional<TemplateField> memConsumption = getField(SystemLoadCalculatedProfile.MEMORY_CONSUMED_FACTOR, templateInfo);
        assertTrue(memConsumption.isPresent());
        assertEquals(0.3333d, Double.valueOf(memConsumption.get().getValue()), delta);

        // CPU avg = (100 + 50)/2
        Optional<TemplateField> cpu = getField(SystemLoadCalculatedProfile.CPU_SPEED, templateInfo);
        assertTrue(cpu.isPresent());
        assertEquals(75d, Double.valueOf(cpu.get().getValue()), delta);

        // MEM consumption factor = (9 + 6)/(100 + 50)
        Optional<TemplateField> cpuConsumption = getField(SystemLoadCalculatedProfile.CPU_CONSUMED_FACTOR, templateInfo);
        assertTrue(cpuConsumption.isPresent());
        assertEquals(0.1d, Double.valueOf(cpuConsumption.get().getValue()), delta);

        // Storage avg = (9 + 5)/2
        Optional<TemplateField> storage = getField(SystemLoadCalculatedProfile.DISK_SIZE, templateInfo);
        assertTrue(storage.isPresent());
        assertEquals(7d, Double.valueOf(storage.get().getValue()), delta);

        // Storage consumption value is set to default value.
        Optional<TemplateField> diskConsumption = getField(SystemLoadCalculatedProfile.DISK_CONSUMED_FACTOR, templateInfo);
        assertTrue(diskConsumption.isPresent());
        assertEquals(SystemLoadCalculatedProfile.STORAGE_CONSUMED_FACTOR_DEFAULT,
                        Double.valueOf(diskConsumption.get().getValue()), delta);

        // NET_THROUGHPUT avg = (21 + 7)/2
        Optional<TemplateField> netThroughput = getField(SystemLoadCalculatedProfile.NETWORK_THROUGHPUT, templateInfo);
        assertTrue(netThroughput.isPresent());
        assertEquals(14d, Double.valueOf(netThroughput.get().getValue()), delta);

        // IO_THROUGHPUT avg = (8 + 24)/2
        Optional<TemplateField> io = getField(SystemLoadCalculatedProfile.IO_THROUGHPUT, templateInfo);
        assertTrue(io.isPresent());
        assertEquals(16d, Double.valueOf(io.get().getValue()), delta);

        // STORAGE_ACCESS avg = (15 + 25)/2
        Optional<TemplateField> accessSpeed = getField(SystemLoadCalculatedProfile.DISK_IOPS, templateInfo);
        assertTrue(accessSpeed.isPresent());
        assertEquals(20d, Double.valueOf(accessSpeed.get().getValue()), delta);
    }


    /**
     * Set commodities across two virtual machines for avg profile
     *  and check for max consumption factor as used > capacity.
     */
    @Test
    public void testCreateVirtualMachineAvgProfileMaxConsumptionFactors() {
        SystemLoadInfoResponse.Builder resp = SystemLoadInfoResponse.newBuilder();
        int clusterId = 1;
        // Records with VM id = 5 and cluster id 1
        resp.addRecord(createRecord(0, 5, clusterId, "VMEM", 50));
        resp.addRecord(createRecord(1, 5, clusterId, "MEM", 100));
        resp.addRecord(createRecord(0, 5, clusterId, "VCPU", 100));
        resp.addRecord(createRecord(1, 5, clusterId, "CPU", 200));
        // Records with VM id = 6 and cluster id 1
        resp.addRecord(createRecord(0, 6, clusterId, "VMEM", 40));
        resp.addRecord(createRecord(1, 6, clusterId, "MEM", 60));
        resp.addRecord(createRecord(0, 6, clusterId, "VCPU", 50));
        resp.addRecord(createRecord(1, 6, clusterId, "CPU", 60));
        SystemLoadCalculatedProfile profile =
                        getSystemLoadCalculatedProfile(Operation.AVG, getClusterWithId(clusterId), resp.build());

        profile.createVirtualMachineProfile();

        assertTrue(profile.getHeadroomTemplateInfo().isPresent());
        TemplateInfo templateInfo = profile.getHeadroomTemplateInfo().get();
        assertEquals(templateInfo.getEntityType(),  EntityType.VIRTUAL_MACHINE_VALUE);

        // MEM consumption factor = (100 + 60) > (50 + 40) : Max consumption factor
        Optional<TemplateField> memConsumption = getField(SystemLoadCalculatedProfile.MEMORY_CONSUMED_FACTOR, templateInfo);
        assertTrue(memConsumption.isPresent());
        assertEquals(1d, Double.valueOf(memConsumption.get().getValue()), delta);

        // MEM consumption factor = (200 + 60) > (100 + 50) : Max consumption factor
        Optional<TemplateField> cpuConsumption = getField(SystemLoadCalculatedProfile.CPU_CONSUMED_FACTOR, templateInfo);
        assertTrue(cpuConsumption.isPresent());
        assertEquals(1d, Double.valueOf(cpuConsumption.get().getValue()), delta);
    }

    /**
     * Set commodities across two virtual machines for max profile and check for max values.
     */
    @Test
    public void testCreateVirtualMachineMaxProfile() {
        SystemLoadInfoResponse.Builder resp = SystemLoadInfoResponse.newBuilder();
        int clusterId = 1;
        // Records with VM id = 5 and cluster id 1
        resp.addRecord(createRecord(0, 5, clusterId, "VMEM", 50));
        resp.addRecord(createRecord(1, 5, clusterId, "MEM", 10));
        resp.addRecord(createRecord(0, 5, clusterId, "VCPU", 100));
        resp.addRecord(createRecord(1, 5, clusterId, "CPU", 9));
        resp.addRecord(createRecord(1, 5, clusterId, "STORAGE_AMOUNT", 9));
        resp.addRecord(createRecord(1, 5, clusterId, "NET_THROUGHPUT", 7));
        resp.addRecord(createRecord(1, 5, clusterId, "IO_THROUGHPUT", 8));
        resp.addRecord(createRecord(1, 5, clusterId, "STORAGE_ACCESS", 15));
        // Records with VM id = 6 and cluster id 1
        resp.addRecord(createRecord(0, 6, clusterId, "VMEM", 40));
        resp.addRecord(createRecord(1, 6, clusterId, "MEM", 20));
        resp.addRecord(createRecord(0, 6, clusterId, "VCPU", 50));
        resp.addRecord(createRecord(1, 6, clusterId, "CPU", 6));
        resp.addRecord(createRecord(1, 6, clusterId, "STORAGE_AMOUNT", 5));
        resp.addRecord(createRecord(1, 6, clusterId, "NET_THROUGHPUT", 21));
        resp.addRecord(createRecord(1, 6, clusterId, "IO_THROUGHPUT", 24));
        resp.addRecord(createRecord(1, 6, clusterId, "STORAGE_ACCESS", 25));
        SystemLoadCalculatedProfile profile =
                        getSystemLoadCalculatedProfile(Operation.MAX, getClusterWithId(clusterId), resp.build());

        profile.createVirtualMachineProfile();

        assertTrue(profile.getHeadroomTemplateInfo().isPresent());
        TemplateInfo templateInfo = profile.getHeadroomTemplateInfo().get();
        assertEquals(templateInfo.getEntityType(),  EntityType.VIRTUAL_MACHINE_VALUE);

        // MEM : max(40,50)
        Optional<TemplateField> mem = getField(SystemLoadCalculatedProfile.MEMORY_SIZE, templateInfo);
        assertTrue(mem.isPresent());
        assertEquals(50d, Double.valueOf(mem.get().getValue()), delta);

        // MEM consumption factor = max(20,10)/max(50,40)
        Optional<TemplateField> memConsumption = getField(SystemLoadCalculatedProfile.MEMORY_CONSUMED_FACTOR, templateInfo);
        assertTrue(memConsumption.isPresent());
        assertEquals(0.4d, Double.valueOf(memConsumption.get().getValue()), delta);

        // CPU : max(100,50)
        Optional<TemplateField> cpu = getField(SystemLoadCalculatedProfile.CPU_SPEED, templateInfo);
        assertTrue(cpu.isPresent());
        assertEquals(100d, Double.valueOf(cpu.get().getValue()), delta);

        // MEM consumption factor = max(9,6)/max(100,50)
        Optional<TemplateField> cpuConsumption = getField(SystemLoadCalculatedProfile.CPU_CONSUMED_FACTOR, templateInfo);
        assertTrue(cpuConsumption.isPresent());
        assertEquals(0.09d, Double.valueOf(cpuConsumption.get().getValue()), delta);

        // Storage : max(9,5)
        Optional<TemplateField> storage = getField(SystemLoadCalculatedProfile.DISK_SIZE, templateInfo);
        assertTrue(storage.isPresent());
        assertEquals(9d, Double.valueOf(storage.get().getValue()), delta);

        // Storage consumption value is set to default value.
        Optional<TemplateField> diskConsumption = getField(SystemLoadCalculatedProfile.DISK_CONSUMED_FACTOR, templateInfo);
        assertTrue(diskConsumption.isPresent());
        assertEquals(SystemLoadCalculatedProfile.STORAGE_CONSUMED_FACTOR_DEFAULT,
                        Double.valueOf(diskConsumption.get().getValue()), delta);

        // NET_THROUGHPUT : max(21,7)
        Optional<TemplateField> netThroughput = getField(SystemLoadCalculatedProfile.NETWORK_THROUGHPUT, templateInfo);
        assertTrue(netThroughput.isPresent());
        assertEquals(21, Double.valueOf(netThroughput.get().getValue()), delta);

        // IO_THROUGHPUT : max(8,24)
        Optional<TemplateField> io = getField(SystemLoadCalculatedProfile.IO_THROUGHPUT, templateInfo);
        assertTrue(io.isPresent());
        assertEquals(24d, Double.valueOf(io.get().getValue()), delta);

        // STORAGE_ACCESS : max(15, 25)
        Optional<TemplateField> accessSpeed = getField(SystemLoadCalculatedProfile.DISK_IOPS, templateInfo);
        assertTrue(accessSpeed.isPresent());
        assertEquals(25d, Double.valueOf(accessSpeed.get().getValue()), delta);
    }

    /**
     * Set commodities across two virtual machines and check for max consumption factor as used > capacity.
     */
    @Test
    public void testCreateVirtualMachineMaxProfileMaxConsumptionFactors() {
        SystemLoadInfoResponse.Builder resp = SystemLoadInfoResponse.newBuilder();
        int clusterId = 1;
        // Records with VM id = 5 and cluster id 1
        resp.addRecord(createRecord(0, 5, clusterId, "VMEM", 50));
        resp.addRecord(createRecord(1, 5, clusterId, "MEM", 100));
        resp.addRecord(createRecord(0, 5, clusterId, "VCPU", 100));
        resp.addRecord(createRecord(1, 5, clusterId, "CPU", 200));
        // Records with VM id = 6 and cluster id 1
        resp.addRecord(createRecord(0, 6, clusterId, "VMEM", 40));
        resp.addRecord(createRecord(1, 6, clusterId, "MEM", 60));
        resp.addRecord(createRecord(0, 6, clusterId, "VCPU", 50));
        resp.addRecord(createRecord(1, 6, clusterId, "CPU", 60));
        SystemLoadCalculatedProfile profile =
                        getSystemLoadCalculatedProfile(Operation.MAX, getClusterWithId(clusterId), resp.build());

        profile.createVirtualMachineProfile();

        assertTrue(profile.getHeadroomTemplateInfo().isPresent());
        TemplateInfo templateInfo = profile.getHeadroomTemplateInfo().get();
        assertEquals(templateInfo.getEntityType(),  EntityType.VIRTUAL_MACHINE_VALUE);

        // MEM consumption factor = (100 + 60) > (50 + 40) : Max consumption factor
        Optional<TemplateField> memConsumption = getField(SystemLoadCalculatedProfile.MEMORY_CONSUMED_FACTOR, templateInfo);
        assertTrue(memConsumption.isPresent());
        assertEquals(1d, Double.valueOf(memConsumption.get().getValue()), delta);

        // MEM consumption factor = (200 + 60) > (100 + 50) : Max consumption factor
        Optional<TemplateField> cpuConsumption = getField(SystemLoadCalculatedProfile.CPU_CONSUMED_FACTOR, templateInfo);
        assertTrue(cpuConsumption.isPresent());
        assertEquals(1d, Double.valueOf(cpuConsumption.get().getValue()), delta);
    }

    private Optional<TemplateField> getField(String name, TemplateInfo templateInfo) {
        return templateInfo.getResourcesList().stream()
        .flatMap(res -> res.getFieldsList().stream())
        .filter(f -> f.getName().equals(name))
        .findFirst();
    }

    private SystemLoadCalculatedProfile getSystemLoadCalculatedProfile(Operation operation, Group cluster,
                    SystemLoadInfoResponse sysLoadResponse) {
        return new SystemLoadCalculatedProfile(operation, cluster, sysLoadResponse,
                        cluster.getCluster().getName(), "");
    }

    private Group getClusterWithId(long clusterId) {
        return Group.newBuilder()
            .setId(clusterId)
            .setCluster(ClusterInfo.newBuilder().setName("Cluster1"))
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
