package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.PhysicalMachineData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;

/**
 *  Populate the {@link TypeSpecificInfo} unique to a PhysicalMachine - i.e. {@link PhysicalMachineInfo}
 **/
public class PhysicalMachineInfoMapper extends TypeSpecificInfoMapper {
    private static final String MACHINE_TYPE_MODEL = "machineTypeAndModel";
    private static final String PROCESSOR_COMPAT_MODES = "supportedProcessorCompatibilityModes";

    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(
            @Nonnull final EntityDTOOrBuilder sdkEntity,
            @Nonnull final Map<String, String> entityPropertyMap) {
        if (!sdkEntity.hasPhysicalMachineData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        final PhysicalMachineData physicalMachineData = sdkEntity.getPhysicalMachineData();
        final PhysicalMachineInfo.Builder physicalMachineInfoBuilder =
                PhysicalMachineInfo.newBuilder();
        // extract each of required the PM properties from the entityPropertyMap (if found)
        if (entityPropertyMap.containsKey(SupplyChainConstants.CPU_MODEL)) {
            physicalMachineInfoBuilder.setCpuModel(entityPropertyMap.get(SupplyChainConstants.CPU_MODEL));
        }
        if (entityPropertyMap.containsKey(SupplyChainConstants.MODEL)) {
            physicalMachineInfoBuilder.setModel(entityPropertyMap.get(SupplyChainConstants.MODEL));
        }
        if (entityPropertyMap.containsKey(SupplyChainConstants.VENDOR)) {
            physicalMachineInfoBuilder.setVendor(entityPropertyMap.get(SupplyChainConstants.VENDOR));
        }
        if (entityPropertyMap.containsKey(SupplyChainConstants.TIME_ZONE)) {
            physicalMachineInfoBuilder.setTimezone(entityPropertyMap.get(SupplyChainConstants.TIME_ZONE));
        }
        if (entityPropertyMap.containsKey(MACHINE_TYPE_MODEL)) {
            physicalMachineInfoBuilder.setModel(entityPropertyMap.get(MACHINE_TYPE_MODEL));
        }
        if (entityPropertyMap.containsKey(PROCESSOR_COMPAT_MODES)) {
            physicalMachineInfoBuilder.setProcessorCompatibilityModes(entityPropertyMap.get(PROCESSOR_COMPAT_MODES));
        }
        if (physicalMachineData.hasNumCpuCores()) {
            physicalMachineInfoBuilder.setNumCpus(physicalMachineData.getNumCpuCores());
        }
        if (physicalMachineData.hasNumCpuSockets()) {
            physicalMachineInfoBuilder.setNumCpuSockets(physicalMachineData.getNumCpuSockets());
        }
        if (physicalMachineData.hasCpuCoreMhz()) {
            physicalMachineInfoBuilder.setCpuCoreMhz(physicalMachineData.getCpuCoreMhz());
        }
        if (physicalMachineData.hasAutomationLevel()) {
            physicalMachineInfoBuilder.setAutomationLevel(physicalMachineData.getAutomationLevel());
        }
        if (physicalMachineData.hasMigrationLevel()) {
            physicalMachineInfoBuilder.setMigrationLevel(physicalMachineData.getMigrationLevel());
        }
        if (physicalMachineData.hasNumCpuThreads()) {
            physicalMachineInfoBuilder.setNumCpuThreads(physicalMachineData.getNumCpuThreads());
        }
        physicalMachineInfoBuilder.addAllDiskGroup(physicalMachineData.getDiskGroupList());

        physicalMachineInfoBuilder.setDedicatedFailover(
                physicalMachineData.getPmState().getFailover());
        // note that the PhysicalMachineInfo will be added even if none of the properties are found
        return TypeSpecificInfo.newBuilder()
                .setPhysicalMachine(physicalMachineInfoBuilder)
                .build();
    }
}
