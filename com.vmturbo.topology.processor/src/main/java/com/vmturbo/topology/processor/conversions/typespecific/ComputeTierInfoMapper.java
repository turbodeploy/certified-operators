package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.VirtualMachineProtoUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.Architecture;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo.SupportedCustomerInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualizationType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ComputeTierData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to a ComputeTier - i.e. {@link ComputeTierInfo}
 **/
public class ComputeTierInfoMapper extends TypeSpecificInfoMapper {

    private static final Logger logger = LogManager.getLogger();
    /**
     * If the compute tier supports 64-bit, then it also supports arm64 VMs which are 64-bit.
     * That is why we return ARM64 as a supported architecture in both cases here.
     */
    private static final Map<String, Set<Architecture>> SUPPORTED_ARCHITECTURES =
            ImmutableMap.of(VirtualMachineProtoUtil.BIT32_OR_BIT64,
                    ImmutableSet.of(Architecture.BIT_32, Architecture.BIT_64, Architecture.ARM_64),
                    VirtualMachineProtoUtil.BIT_64,
                    ImmutableSet.of(Architecture.BIT_64, Architecture.ARM_64));

    private static final Map<String, Set<VirtualizationType>> SUPPORTED_VIRTUALIZATION_TYPES =
            ImmutableMap.of(VirtualMachineProtoUtil.PVM_AND_HVM,
                    ImmutableSet.of(VirtualizationType.HVM, VirtualizationType.PVM),
                    VirtualMachineProtoUtil.HVM_ONLY, ImmutableSet.of(VirtualizationType.HVM));

    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(
            @Nonnull final EntityDTOOrBuilder sdkEntity,
            @Nonnull final Map<String, String> entityPropertyMap) {
        if (!sdkEntity.hasComputeTierData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        SupportedCustomerInfo.Builder supportedCustomerInfo = SupportedCustomerInfo.newBuilder();
        String enhancedNetworkingType = entityPropertyMap.get(VirtualMachineProtoUtil.PROPERTY_ENHANCED_NETWORKING_TYPE);
        if (StringUtils.isNotEmpty(enhancedNetworkingType)) {
            supportedCustomerInfo.setSupportsOnlyEnaVms(
                    VirtualMachineProtoUtil.PROPERTY_VALUE_ENA.equals(enhancedNetworkingType));
        }
        String requiresNvme = entityPropertyMap.get(VirtualMachineProtoUtil.PROPERTY_REQUIRES_NVME);
        if (StringUtils.isNotEmpty(requiresNvme)) {
            supportedCustomerInfo.setSupportsOnlyNVMeVms(Boolean.parseBoolean(requiresNvme));
        }
        supportedCustomerInfo.addAllSupportedArchitectures(parseArchitecture(sdkEntity, entityPropertyMap));
        supportedCustomerInfo.addAllSupportedVirtualizationTypes(parseVirtualizationType(sdkEntity, entityPropertyMap));

        final ComputeTierData ctData = sdkEntity.getComputeTierData();
        ComputeTierInfo.Builder computeTierInfoBuilder = ComputeTierInfo.newBuilder()
                .setFamily(ctData.getFamily())
                .setQuotaFamily(ctData.getQuotaFamily())
                .setDedicatedStorageNetworkState(ctData.getDedicatedStorageNetworkState())
                .setNumCoupons(ctData.getNumCoupons())
                .setNumCores(ctData.getNumCores())
                .setBurstableCPU(ctData.getBurstableCPU())
                .setSupportedCustomerInfo(supportedCustomerInfo);
        if (ctData.hasInstanceDiskSizeGb()) {
            computeTierInfoBuilder.setInstanceDiskSizeGb(ctData.getInstanceDiskSizeGb());
        }
        if (ctData.hasInstanceDiskType()) {
            computeTierInfoBuilder.setInstanceDiskType(ctData.getInstanceDiskType());
        }
        if (ctData.hasNumInstanceDisks()) {
            computeTierInfoBuilder.setNumInstanceDisks(ctData.getNumInstanceDisks());
        }
        return TypeSpecificInfo.newBuilder()
                .setComputeTier(computeTierInfoBuilder.build())
                .build();
    }

    private Set<Architecture> parseArchitecture(@Nonnull final EntityDTOOrBuilder sdkEntity,
                                                @Nonnull final Map<String, String> entityPropertyMap) {
        String architecture = entityPropertyMap.get(VirtualMachineProtoUtil.PROPERTY_ARCHITECTURE);
        if (StringUtils.isEmpty(architecture)) {
            return Collections.emptySet();
        }
        final Set<Architecture> supportedArchitectures = SUPPORTED_ARCHITECTURES.get(architecture);
        if (supportedArchitectures != null) {
            return supportedArchitectures;
        } else {
            logger.error("Unknown supported architecture - {}. Could not add " +
                            "supported architecture in ComputeTierInfo for {}", architecture,
                    sdkEntity.getDisplayName());
            return Collections.emptySet();
        }
    }

    private Set<VirtualizationType> parseVirtualizationType(@Nonnull final EntityDTOOrBuilder sdkEntity,
                                                            @Nonnull final Map<String, String> entityPropertyMap) {
        String virtualizationType = entityPropertyMap.get(
                VirtualMachineProtoUtil.PROPERTY_SUPPORTED_VIRTUALIZATION_TYPE);
        if (StringUtils.isEmpty(virtualizationType)) {
            return Collections.emptySet();
        }
        final Set<VirtualizationType> virtualizationTypes =
                SUPPORTED_VIRTUALIZATION_TYPES.get(virtualizationType);
        if (virtualizationTypes != null) {
            return virtualizationTypes;
        } else {
            logger.error("Unknown supported virtualization type - {}. Could not add " +
                            "supported virtualization type in ComputeTierInfo for {}", virtualizationType,
                    sdkEntity.getDisplayName());
            return Collections.emptySet();
        }
    }
}
