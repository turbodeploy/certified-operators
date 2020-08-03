package com.vmturbo.common.protobuf;

import java.util.Map;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.Architecture;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualizationType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Miscellaneous utilities for messages defined in VirtualMachineInfo/TopologyDTO.proto.
 */
public class VirtualMachineProtoUtil {

    /**
     * Key whose value is associated with enhanced networking type.
     */
    public static final String PROPERTY_ENHANCED_NETWORKING_TYPE = "enhancedNetworkingType";

    /**
     * Key whose value is associated with ENA.
     */
    public static final String PROPERTY_VALUE_ENA = "ENA";

    /**
     * A property that is sent from the API, if ENA is supported.
     */
    public static final String ENA_IS_ACTIVE = "Active";

    /**
     * A property that is sent from the API, if ENA is not supported.
     */
    public static final String ENA_IS_NOT_ACTIVE = "Inactive";

    /**
     * Key whose value is associated with NVMe required flag.
     */
    public static final String PROPERTY_REQUIRES_NVME = "NVMeRequired";

    /**
     * Key whose value is associated with architecture.
     */
    public static final String PROPERTY_ARCHITECTURE = "architecture";

    /**
     * Key whose value is associated with supported virtualization type.
     */
    public static final String PROPERTY_SUPPORTED_VIRTUALIZATION_TYPE =
            "supportedVirtualizationType";

    /**
     * 32-bit or 64-bit architecture.
     */
    public static final String BIT32_OR_BIT64 = "32-bit or 64-bit";

    /**
     * 64-bit architecture.
     */
    public static final String BIT_64 = "64-bit";

    /**
     * 32-bit architecture.
     */
    public static final String BIT_32 = "32-bit";

    /**
     * arm64 architecture type.
     */
    public static final String ARM_64 = "arm64";

    /**
     * PVM and HVM virtualization type.
     */
    public static final String PVM_AND_HVM = "PVM_AND_HVM";

    /**
     * HVM virtualization type only.
     */
    public static final String HVM_ONLY = "HVM_ONLY";

    /**
     * HVM virtualization type.
     */
    public static final String HVM = "HVM";

    /**
     * PVM virtualization type.
     */
    public static final String PVM = "PVM";

    /**
     * Default/shared tenancy type both refer to DEFAULT.
     */
    public static final String SHARED = "shared";

    /**
     * Default/shared tenancy type both refer to DEFAULT.
     */
    public static final String DEFAULT = "default";

    /**
     * Dedicated tenancy type.
     */
    public static final String DEDICATED = "dedicated";

    /**
     * Host tenancy type.
     */
    public static final String HOST = "host";

    /**
     * Key whose value is associated with ENA supported flag.
     */
    public static final String PROPERTY_IS_ENA_SUPPORTED = "isEnaSupported";

    /**
     * Key whose value is associated with NVMe supported flag.
     */
    public static final String PROPERTY_SUPPORTS_NVME = "NVMe";

    /**
     * Key whose value is associated with virtualization type.
     */
    public static final String PROPERTY_VM_VIRTUALIZATION_TYPE = "virtualizationType";

    /**
     * Info about any read-only lock prerequisites on VM preventing action execution.
     */
    public static final String PROPERTY_LOCKS = "locks";

    /**
     * String representation of architecture type to protobuf type.
     */
    public static final BiMap<String, Architecture> ARCHITECTURE =
            ImmutableBiMap.of(
                    BIT_64, Architecture.BIT_64,
                    BIT_32, Architecture.BIT_32,
                    ARM_64, Architecture.ARM_64);

    /**
     * String representation of virtualization type to protobuf type.
     */
    public static final BiMap<String, VirtualizationType> VIRTUALIZATION_TYPE =
            ImmutableBiMap.of(
                    HVM, VirtualizationType.HVM,
                    PVM, VirtualizationType.PVM);

    /**
     * Key whose value is associated with virtualization type.
     */
    public static final String PROPERTY_VM_TENANCY = "tenancy";

    /**
     * String representation of tenancy to protobuf type.
     */
    public static final Map<String, Tenancy> TENANCY =
        ImmutableMap.of(
            DEFAULT, Tenancy.DEFAULT,
            SHARED, Tenancy.DEFAULT,
            DEDICATED, Tenancy.DEDICATED,
            HOST, Tenancy.HOST);

    private VirtualMachineProtoUtil() {}
}
