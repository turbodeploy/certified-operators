package com.vmturbo.topology.processor.conversions.typespecific;

import static com.vmturbo.components.common.utils.StringConstants.BIT32_OR_BIT64;
import static com.vmturbo.components.common.utils.StringConstants.BIT_64;
import static com.vmturbo.components.common.utils.StringConstants.HVM_ONLY;
import static com.vmturbo.components.common.utils.StringConstants.PROPERTY_ARCHITECTURE;
import static com.vmturbo.components.common.utils.StringConstants.PROPERTY_ENHANCED_NETWORKING_TYPE;
import static com.vmturbo.components.common.utils.StringConstants.PROPERTY_REQUIRES_NVME;
import static com.vmturbo.components.common.utils.StringConstants.PROPERTY_SUPPORTED_VIRTUALIZATION_TYPE;
import static com.vmturbo.components.common.utils.StringConstants.PROPERTY_VALUE_ENA;
import static com.vmturbo.components.common.utils.StringConstants.PVM_AND_HVM;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(
            @Nonnull final EntityDTOOrBuilder sdkEntity,
            @Nonnull final Map<String, String> entityPropertyMap) {
        if (!sdkEntity.hasComputeTierData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        SupportedCustomerInfo.Builder supportedCustomerInfo = SupportedCustomerInfo.newBuilder();
        String enhancedNetworkingType = entityPropertyMap.get(PROPERTY_ENHANCED_NETWORKING_TYPE);
        if (enhancedNetworkingType != null && !enhancedNetworkingType.isEmpty()) {
            supportedCustomerInfo.setSupportsOnlyEnaVms(PROPERTY_VALUE_ENA.equals(enhancedNetworkingType) ? true : false);
        }
        String requiresNvme = entityPropertyMap.get(PROPERTY_REQUIRES_NVME);
        if (requiresNvme != null && !requiresNvme.isEmpty()) {
            supportedCustomerInfo.setSupportsOnlyNVMeVms(Boolean.parseBoolean(requiresNvme));
        }
        supportedCustomerInfo.addAllSupportedArchitectures(parseArchitecture(sdkEntity, entityPropertyMap));
        supportedCustomerInfo.addAllSupportedVirtualizationTypes(parseVirtualizationType(sdkEntity, entityPropertyMap));

        final ComputeTierData ctData = sdkEntity.getComputeTierData();
        return TypeSpecificInfo.newBuilder()
                .setComputeTier(ComputeTierInfo.newBuilder()
                        .setFamily(ctData.getFamily())
                        .setDedicatedStorageNetworkState(ctData.getDedicatedStorageNetworkState())
                        .setNumCoupons(ctData.getNumCoupons())
                        .setNumCores(ctData.getNumCores())
                        .setSupportedCustomerInfo(supportedCustomerInfo)
                        .build())
                .build();
    }

    private Set<Architecture> parseArchitecture(@Nonnull final EntityDTOOrBuilder sdkEntity,
                                                @Nonnull final Map<String, String> entityPropertyMap) {
        String architecture = entityPropertyMap.get(PROPERTY_ARCHITECTURE);
        if (architecture == null || architecture.isEmpty()) {
            return Collections.emptySet();
        }
        // If the compute tier supports 64-bit, then it also supports arm64 VMs which are
        // 64-bit. That is why we return ARM64 as a supported architecture in both cases here.
        switch (architecture) {
            case BIT32_OR_BIT64:
                return ImmutableSet.of(Architecture.BIT_32, Architecture.BIT_64, Architecture.ARM_64);
            case BIT_64:
                return ImmutableSet.of(Architecture.BIT_64, Architecture.ARM_64);
            default:
                logger.error("Unknown supported architecture - {}. Could not add " +
                    "supported architecture in ComputeTierInfo for {}", architecture,
                    sdkEntity.getDisplayName());
                return Collections.emptySet();
        }
    }

    private Set<VirtualizationType> parseVirtualizationType(@Nonnull final EntityDTOOrBuilder sdkEntity,
                                                            @Nonnull final Map<String, String> entityPropertyMap) {
        String virtualizationType = entityPropertyMap.get(PROPERTY_SUPPORTED_VIRTUALIZATION_TYPE);
        if (virtualizationType == null || virtualizationType.isEmpty()) {
            return Collections.emptySet();
        }
        switch (virtualizationType) {
            case PVM_AND_HVM:
                return ImmutableSet.of(VirtualizationType.HVM, VirtualizationType.PVM);
            case HVM_ONLY:
                return ImmutableSet.of(VirtualizationType.HVM);
            default:
                logger.error("Unknown supported virtualization type - {}. Could not add " +
                    "supported virtualization type in ComputeTierInfo for {}", virtualizationType,
                    sdkEntity.getDisplayName());
                return Collections.emptySet();
        }
    }
}
