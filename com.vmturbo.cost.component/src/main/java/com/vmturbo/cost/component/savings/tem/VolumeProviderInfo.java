package com.vmturbo.cost.component.savings.tem;

import java.util.Map;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Provider Info for volume entity type.
 */
public class VolumeProviderInfo extends BaseProviderInfo {

    /**
     * Constructor. Create ProviderInfo from discovered entity topology DTO.
     *
     * @param cloudTopology cloud topology
     * @param discoveredEntity discovered entity
     */
    public VolumeProviderInfo(CloudTopology<TopologyEntityDTO> cloudTopology,
            TopologyEntityDTO discoveredEntity) {
        super(cloudTopology, discoveredEntity, CloudTopology::getStorageTier, ImmutableSet.of(
                CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE,
                CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE,
                CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE));
    }

    /**
     * Constructor.
     *
     * @param providerOid provider OID
     * @param commodityCapacities commodity capacities
     */
    public VolumeProviderInfo(Long providerOid, Map<Integer, Double> commodityCapacities) {
        super(providerOid, commodityCapacities, EntityType.VIRTUAL_VOLUME_VALUE);
    }
}
