package com.vmturbo.plan.orchestrator.project.headroom;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadInfoResponse;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadRecord;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Calculates system load based on given operation type and provides template information.
 * Template information is created by processing fetched system load records
 * and calculate average/max of commodity values across cluster's VMs records.
 */
public class SystemLoadCalculatedProfile {

    // compute fields which relate to template's "ResourceCategoryName.Compute".
    public static final String CPU_CONSUMED_FACTOR = "cpuConsumedFactor";
    public static final String CPU_SPEED = "cpuSpeed";
    public static final String IO_THROUGHPUT = "ioThroughput";
    public static final String IO_THROUGHPUT_SIZE = "ioThroughputSize";
    public static final String MEMORY_CONSUMED_FACTOR = "memoryConsumedFactor";
    public static final String MEMORY_SIZE = "memorySize";
    public static final String NUM_OF_CPU = "numOfCpu";
    public static final String NUM_OF_CORES = "numOfCores";
    public static final String NETWORK_THROUGHPUT = "networkThroughput";
    public static final String NETWORK_THROUGHPUT_SIZE = "networkThroughputSize";
    // storage fields which relate to template's "ResourceCategoryName.Storage".
    public static final String DISK_IOPS = "diskIops";
    public static final String DISK_SIZE = "diskSize";
    public static final String DISK_CONSUMED_FACTOR = "diskConsumedFactor";
    // default consumption factor
    public static final float MEM_CONSUMED_FACTOR_DEFAULT = 0.75F;
    public static final float STORAGE_CONSUMED_FACTOR_DEFAULT = 1.0F;
    public static final float CPU_CONSUMED_FACTOR_DEFAULT = 0.5F;

    // Compute stats have field name as key which relates to template's "ResourceCategoryName.Compute"
    // and value as read/calculated from system load.
    private final Map<String, Float> templateComputeStats = new HashMap<>();

    // Storage stats have field name as key which relates to template's "ResourceCategoryName.Storage"
    // and value as read/calculated from system load.
    private final Map<String, Float> templateStorageStats = new HashMap<>();

    // A map with VM uuid as key and records that belong to this uuid as value.
    private Multimap<String, SystemLoadRecord> virtualMachinesMap = ArrayListMultimap.create();

    private Operation operation = null;
    private Group cluster = null;
    private String profileNamePostfix = null;
    private String profileDisplayNamePostfix = null;
    private Optional<TemplateInfo> headroomTemplateInfo = Optional.empty();

    /**
     * Create a VM template with the average/max of the values for all VMs in the cluster.
     */
    public enum Operation {
        AVG,
        MAX
    }

    /**
     * Create SystemLoadCalculatedProfile instance.
     *
     * @param operation can be MAX or AVG.
     * @param cluster for which we want to calculate profile and create template info for.
     * @param records to process for system load calculation.
     * @param profileNamePostfix used to generate profile name.
     * @param profileDisplayNamePostfix used to generate profile name.
     */
    public SystemLoadCalculatedProfile(final Operation operation, final Group cluster, final SystemLoadInfoResponse records,
                    final String profileNamePostfix, final String profileDisplayNamePostfix) {
        this.operation = operation;
        this.cluster = cluster;
        this.profileNamePostfix = profileNamePostfix;
        this.profileDisplayNamePostfix = profileDisplayNamePostfix;
        createVirtualMachinesMap(records);
    }

    /**
     * A map with VM uuid as key and records that belong to this uuid as value.
     *
     * @param records to process.
     */
    private void createVirtualMachinesMap(final SystemLoadInfoResponse records) {
        if (records != null) {
            List<SystemLoadRecord> recList = records.getRecordList();
            for (SystemLoadRecord rec : recList) {
                if (rec.getClusterId() == cluster.getId()) {
                    String vmId = Long.toString(rec.getUuid());
                    virtualMachinesMap.put(vmId, rec);
                }
            }
        }
    }

    /**
     * Based on operation type (AVG or MAX), iterate for all records for VMs' commodities and set values.
     * Use these values to create template info.
     */
    public void createVirtualMachineProfile() {
        // Assigning initial values to all of the variables to be placed in vm profile
        float vMemSize = 0;
        float vCPUSpeed = 0;
        float memConsumed = 0;
        float cpuConsumed = 0;
        float networkThroughputConsumed = 0;
        float ioThroughputConsumed = 0;
        float storageConsumed = 0;
        float memConsumedFactor = 0;
        float cpuConsumedFactor = 0;
        float storageConsumedFactor = 0;
        int numVMs = 0;

        // Iterate through VMs to find average or peak of commodities
        for (String vmId : virtualMachinesMap.keySet()) {
            numVMs++;

            float currVMemCapacity = 0;
            float currVCPUCapacity = 0;
            float currMemConsumed = 0;
            float currCPUConsumed = 0;
            float currNetworkThroughputConsumed = 0;
            float currIoThroughputConsumed = 0;
            float currStorageConsumed = 0;

            for (SystemLoadRecord rec : virtualMachinesMap.get(vmId)) {
                if (rec.getPropertySubtype().equals("used")) {
                    if (rec.getRelationType() == 0) {
                        // sold commodities
                        switch (rec.getPropertyType()) {
                            case "VMEM":
                                currVMemCapacity = (float)rec.getCapacity();
                                break;
                            case "VCPU":
                                currVCPUCapacity = (float)rec.getCapacity();
                                break;
                            case "VSTORAGE":
                                currStorageConsumed = (float)rec.getCapacity();
                                break;
                        }

                    }  else if (rec.getRelationType() == 1) {
                        //bought commodities
                        switch (rec.getPropertyType()) {
                            case "MEM":
                                currMemConsumed = (float)rec.getAvgValue();
                                break;
                            case "CPU":
                                currCPUConsumed = (float)rec.getAvgValue();
                                break;
                            case "NET_THROUGHPUT":
                                currNetworkThroughputConsumed = (float)rec.getAvgValue();
                                break;
                            case "IO_THROUGHPUT":
                                currIoThroughputConsumed = (float)rec.getAvgValue();
                                break;
                        }
                    }
                }
            }

            switch (operation) {
                case AVG:
                    vMemSize += currVMemCapacity;
                    vCPUSpeed += currVCPUCapacity;
                    memConsumed += currMemConsumed;
                    cpuConsumed += currCPUConsumed;
                    networkThroughputConsumed += currNetworkThroughputConsumed;
                    ioThroughputConsumed += currIoThroughputConsumed;
                    storageConsumed += currStorageConsumed;
                    break;
                case MAX:
                    vMemSize = Math.max(vMemSize, currVMemCapacity);
                    vCPUSpeed = Math.max(vCPUSpeed, currVCPUCapacity);
                    memConsumed = Math.max(memConsumed, currMemConsumed);
                    cpuConsumed = Math.max(cpuConsumed, currCPUConsumed);
                    networkThroughputConsumed = Math.max(networkThroughputConsumed, currNetworkThroughputConsumed);
                    ioThroughputConsumed = Math.max(ioThroughputConsumed, currIoThroughputConsumed);
                    storageConsumed = Math.max(storageConsumed, currStorageConsumed);
                    memConsumedFactor = Math.max(memConsumedFactor, Float.isFinite(currMemConsumed / currVMemCapacity) ?
                                    currMemConsumed / currVMemCapacity : MEM_CONSUMED_FACTOR_DEFAULT);
                    cpuConsumedFactor = Math.max(cpuConsumedFactor, Float.isFinite(currCPUConsumed / currVCPUCapacity) ?
                                    currCPUConsumed / currVCPUCapacity : CPU_CONSUMED_FACTOR_DEFAULT);
                    storageConsumedFactor = STORAGE_CONSUMED_FACTOR_DEFAULT;
                    break;
            }
        }

        if (operation == Operation.AVG && numVMs > 0) {
            // find average instead of sum for variables for average profile
            memConsumedFactor = Float.isFinite(memConsumed / vMemSize) ?
                                memConsumed / vMemSize : MEM_CONSUMED_FACTOR_DEFAULT;
            cpuConsumedFactor = Float.isFinite(cpuConsumed / vCPUSpeed) ?
                            cpuConsumed / vCPUSpeed : CPU_CONSUMED_FACTOR_DEFAULT;
            storageConsumedFactor = STORAGE_CONSUMED_FACTOR_DEFAULT;
            memConsumed = memConsumed / numVMs;
            vMemSize = vMemSize / numVMs;
            cpuConsumed = cpuConsumed / numVMs;
            vCPUSpeed = vCPUSpeed / numVMs;
            networkThroughputConsumed = networkThroughputConsumed / numVMs;
            ioThroughputConsumed = ioThroughputConsumed / numVMs;
            storageConsumed = storageConsumed / numVMs;
        }

        // compute
        templateComputeStats.put(CPU_CONSUMED_FACTOR, Math.min(1, cpuConsumedFactor));
        templateComputeStats.put(CPU_SPEED, vCPUSpeed);
        // TODO : Update NUM_OF_CPU once we are able to extract this information.
        templateComputeStats.put(NUM_OF_CPU, 1F);
        templateComputeStats.put(IO_THROUGHPUT, ioThroughputConsumed);
        templateComputeStats.put(MEMORY_CONSUMED_FACTOR, Math.min(1, memConsumedFactor));
        templateComputeStats.put(MEMORY_SIZE, vMemSize);
        templateComputeStats.put(NETWORK_THROUGHPUT, networkThroughputConsumed);
        // storage
        templateStorageStats.put(DISK_CONSUMED_FACTOR, Math.min(1, storageConsumedFactor));
        templateStorageStats.put(DISK_SIZE, storageConsumed);
        setHeadroomTemplateInfo(generateTemplateInfoFromProfile());
    }

    /**
     * Provides template information based on system profile.
     * @return template info based on calculated system load values.
     */
    private Optional<TemplateInfo> generateTemplateInfoFromProfile() {
        TemplateInfo.Builder templateInfo = TemplateInfo.newBuilder();
        //replace \ with _ in profile name, so it can be used in REST API as parameter
        templateInfo.setName(operation.name() + ":" + profileDisplayNamePostfix.replaceAll("\\\\", "_"));
        templateInfo.setDescription(operation.name() + ":" + profileNamePostfix.replaceAll("\\\\", "_"));
        templateInfo.setEntityType(EntityType.VIRTUAL_MACHINE_VALUE);
        templateInfo.setTemplateSpecId(1);
        final ImmutableList.Builder<TemplateResource> templateResources = ImmutableList.builder();
        templateResources.add(TemplateResource.newBuilder()
            .setCategory(ResourcesCategory.newBuilder().setName(ResourcesCategoryName.Compute))
            .addAllFields(getFieldsFromStats(templateComputeStats))
            .build());
        templateResources.add(TemplateResource.newBuilder()
                        .setCategory(ResourcesCategory.newBuilder().setName(ResourcesCategoryName.Storage))
                        .addAllFields(getFieldsFromStats(templateStorageStats))
                        .build());
        templateInfo.addAllResources(templateResources.build());
        return Optional.ofNullable(templateInfo.build());
    }

    private ImmutableList<TemplateField> getFieldsFromStats(Map<String, Float> templateStats) {
        final ImmutableList.Builder<TemplateField> fields = ImmutableList.builder();
        templateStats.forEach((name, value) -> fields.add(TemplateField.newBuilder()
            .setName(name)
            .setValue(String.valueOf(value))
            .build()));
        return fields.build();
    }

    private void setHeadroomTemplateInfo(Optional<TemplateInfo> headroomTemplateInfo) {
        this.headroomTemplateInfo = headroomTemplateInfo;
    }

    public Optional<TemplateInfo> getHeadroomTemplateInfo() {
        return headroomTemplateInfo;
    }
}
