package com.vmturbo.topology.processor.conversions.typespecific;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;

/**
 *  Populate the {@link TypeSpecificInfo} unique to a PhysicalMachine - i.e. {@link PhysicalMachineInfo}
 **/
public class PhysicalMachineInfoMapper extends TypeSpecificInfoMapper {
    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(final EntityDTOOrBuilder sdkEntity) {
        if (!sdkEntity.hasPhysicalMachineData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        final PhysicalMachineInfo.Builder physicalMachineInfoBuilder =
                PhysicalMachineInfo.newBuilder();
        // extract each of required the PM properties from the given sdkEntity (if found)
        getEntityPropertyValue(sdkEntity, SupplyChainConstants.CPU_MODEL)
                .map(physicalMachineInfoBuilder::setCpuModel);
        getEntityPropertyValue(sdkEntity, SupplyChainConstants.MODEL)
                .map(physicalMachineInfoBuilder::setModel);
        getEntityPropertyValue(sdkEntity, SupplyChainConstants.VENDOR)
                .map(physicalMachineInfoBuilder::setVendor);
        // note that the PhysicalMachineInfo will be added even if none of the properties are found
        return TypeSpecificInfo.newBuilder()
                .setPhysicalMachine(physicalMachineInfoBuilder)
                .build();

    }
}
