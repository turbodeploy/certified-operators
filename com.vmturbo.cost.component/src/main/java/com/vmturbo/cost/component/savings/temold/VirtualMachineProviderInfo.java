package com.vmturbo.cost.component.savings.temold;

import java.util.HashMap;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * ProviderInfo for VM.
 */
public class VirtualMachineProviderInfo extends BaseProviderInfo {

    /**
     * Constructor. Create provider info from discovered topology entity DTO.
     *
     * @param cloudTopology cloud topology
     * @param discoveredEntity discovered entity
     */
    public VirtualMachineProviderInfo(CloudTopology<TopologyEntityDTO> cloudTopology,
            TopologyEntityDTO discoveredEntity) {
        super(cloudTopology, discoveredEntity, CloudTopology::getComputeTier, ImmutableSet.of());
    }

    /**
     * Constructor.
     *
     * @param providerOid provider OID
     */
    public VirtualMachineProviderInfo(Long providerOid) {
        super(providerOid, new HashMap<>(), EntityType.VIRTUAL_MACHINE_VALUE);
    }
}
