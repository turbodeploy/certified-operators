package com.vmturbo.plan.orchestrator.project.headroom;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import com.vmturbo.common.protobuf.TemplateProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadRecord;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Calculates system load based on given operation type and provides template information.
 * Template information is created by processing fetched system load records
 * and calculate average/max of commodity values across cluster's VMs records.
 */
public class SystemLoadCalculatedProfile {

    // Compute stats have field name as key which relates to template's "ResourceCategoryName.Compute"
    // and value as read/calculated from system load.
    private final Map<String, Float> templateComputeStats = new HashMap<>();

    // Storage stats have field name as key which relates to template's "ResourceCategoryName.Storage"
    // and value as read/calculated from system load.
    private final Map<String, Float> templateStorageStats = new HashMap<>();

    // A map with VM uuid as key and records that belong to this uuid as value.
    private Multimap<String, SystemLoadRecord> virtualMachinesMap = ArrayListMultimap.create();

    private Operation operation = null;
    private Grouping cluster = null;
    private String profileNamePostfix = null;
    private String profileDisplayNamePostfix = null;
    private String profileName = null;
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
     * @param systemLoadRecordList to process for system load calculation.
     * @param profileNamePostfix used to generate profile name.
     * @param profileDisplayNamePostfix used to generate profile name.
     */
    SystemLoadCalculatedProfile(
            @Nonnull final Operation operation, @Nonnull final Grouping cluster,
            @Nonnull final List<SystemLoadRecord> systemLoadRecordList,
            final String profileNamePostfix, final String profileDisplayNamePostfix) {
        this.operation = operation;
        this.cluster = cluster;
        this.profileNamePostfix = profileNamePostfix;
        this.profileDisplayNamePostfix = profileDisplayNamePostfix;
        createVirtualMachinesMap(systemLoadRecordList);
    }

    /**
     * A map with VM uuid as key and records that belong to this uuid as value.
     *
     * @param systemLoadRecordList to process.
     */
    private void createVirtualMachinesMap(@Nonnull final List<SystemLoadRecord> systemLoadRecordList) {
        for (SystemLoadRecord record : systemLoadRecordList) {
            if (record.getClusterId() == cluster.getId()) {
                String vmId = Long.toString(record.getUuid());
                virtualMachinesMap.put(vmId, record);
            }
        }
    }

    /**
     * Based on operation type (AVG or MAX), iterate for all records for VMs' commodities and set values.
     * Use these values to create template info.
     */
    void createVirtualMachineProfile() {
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
        float accessSpeedConsumed = 0;
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
            float currAccessSpeedConsumed = 0;

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
                            case "STORAGE_AMOUNT":
                                currStorageConsumed = (float)rec.getAvgValue();
                                break;
                            case "STORAGE_ACCESS":
                                currAccessSpeedConsumed = (float)rec.getAvgValue();
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
                    accessSpeedConsumed += currAccessSpeedConsumed;
                    break;
                case MAX:
                    vMemSize = Math.max(vMemSize, currVMemCapacity);
                    vCPUSpeed = Math.max(vCPUSpeed, currVCPUCapacity);
                    memConsumed = Math.max(memConsumed, currMemConsumed);
                    cpuConsumed = Math.max(cpuConsumed, currCPUConsumed);
                    networkThroughputConsumed = Math.max(networkThroughputConsumed, currNetworkThroughputConsumed);
                    ioThroughputConsumed = Math.max(ioThroughputConsumed, currIoThroughputConsumed);
                    storageConsumed = Math.max(storageConsumed, currStorageConsumed);
                    accessSpeedConsumed = Math.max(accessSpeedConsumed, currAccessSpeedConsumed);
                    memConsumedFactor = Math.max(memConsumedFactor, Float.isFinite(currMemConsumed / currVMemCapacity) ?
                        currMemConsumed / currVMemCapacity : TemplateProtoUtil.VM_COMPUTE_MEM_CONSUMED_FACTOR_DEFAULT_VALUE);
                    cpuConsumedFactor = Math.max(cpuConsumedFactor, Float.isFinite(currCPUConsumed / currVCPUCapacity) ?
                        currCPUConsumed / currVCPUCapacity : TemplateProtoUtil.VM_COMPUTE_CPU_CONSUMED_FACTOR_DEFAULT_VALUE);
                    storageConsumedFactor = TemplateProtoUtil.VM_STORAGE_DISK_CONSUMED_FACTOR_DEFAULT_VALUE;
                    break;
            }
        }

        if (operation == Operation.AVG && numVMs > 0) {
            // find average instead of sum for variables for average profile
            memConsumedFactor = Float.isFinite(memConsumed / vMemSize) ?
                memConsumed / vMemSize : TemplateProtoUtil.VM_COMPUTE_MEM_CONSUMED_FACTOR_DEFAULT_VALUE;
            cpuConsumedFactor = Float.isFinite(cpuConsumed / vCPUSpeed) ?
                cpuConsumed / vCPUSpeed : TemplateProtoUtil.VM_COMPUTE_CPU_CONSUMED_FACTOR_DEFAULT_VALUE;
            storageConsumedFactor = TemplateProtoUtil.VM_STORAGE_DISK_CONSUMED_FACTOR_DEFAULT_VALUE;
            memConsumed = memConsumed / numVMs;
            vMemSize = vMemSize / numVMs;
            cpuConsumed = cpuConsumed / numVMs;
            vCPUSpeed = vCPUSpeed / numVMs;
            networkThroughputConsumed = networkThroughputConsumed / numVMs;
            ioThroughputConsumed = ioThroughputConsumed / numVMs;
            storageConsumed = storageConsumed / numVMs;
            accessSpeedConsumed = accessSpeedConsumed / numVMs;
        }

        // compute
        templateComputeStats.put(TemplateProtoUtil.VM_COMPUTE_CPU_CONSUMED_FACTOR, Math.min(1, cpuConsumedFactor));
        templateComputeStats.put(TemplateProtoUtil.VM_COMPUTE_VCPU_SPEED, vCPUSpeed);
        // TODO : Update NUM_OF_CPU once we are able to extract this information.
        templateComputeStats.put(TemplateProtoUtil.VM_COMPUTE_NUM_OF_VCPU, 1F);
        templateComputeStats.put(TemplateProtoUtil.VM_COMPUTE_IO_THROUGHPUT_SIZE, ioThroughputConsumed);
        templateComputeStats.put(TemplateProtoUtil.VM_COMPUTE_MEM_CONSUMED_FACTOR, Math.min(1, memConsumedFactor));
        templateComputeStats.put(TemplateProtoUtil.VM_COMPUTE_MEM_SIZE, vMemSize);
        templateComputeStats.put(TemplateProtoUtil.VM_COMPUTE_NETWORK_THROUGHPUT_SIZE, networkThroughputConsumed);
        // storage
        templateStorageStats.put(TemplateProtoUtil.VM_STORAGE_DISK_CONSUMED_FACTOR, Math.min(1, storageConsumedFactor));
        templateStorageStats.put(TemplateProtoUtil.VM_STORAGE_DISK_SIZE, storageConsumed);
        templateStorageStats.put(TemplateProtoUtil.VM_STORAGE_DISK_IOPS, accessSpeedConsumed);
        setHeadroomTemplateInfo(generateTemplateInfoFromProfile());
    }

    /**
     * Provides template information based on system profile.
     * @return template info based on calculated system load values.
     */
    private Optional<TemplateInfo> generateTemplateInfoFromProfile() {
        TemplateInfo.Builder templateInfo = TemplateInfo.newBuilder();
        //replace \ with _ in profile name, so it can be used in REST API as parameter
        String name = operation.name() + ":" + profileDisplayNamePostfix.replaceAll("\\\\", "_");
        this.profileName = name;
        templateInfo.setName(name);
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

    public String getProfileName() {
        return profileName;
    }
}
