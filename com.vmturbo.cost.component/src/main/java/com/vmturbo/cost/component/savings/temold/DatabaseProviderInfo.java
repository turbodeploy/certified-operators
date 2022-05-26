package com.vmturbo.cost.component.savings.temold;

import java.util.Map;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Provider Info for database entity type.
 */
public class DatabaseProviderInfo extends BaseProviderInfo {

    /**
     * Constructor.
     *
     * @param cloudTopology cloud topology
     * @param discoveredEntity discovered entity
     */
    public DatabaseProviderInfo(CloudTopology<TopologyEntityDTO> cloudTopology,
            TopologyEntityDTO discoveredEntity) {
        super(cloudTopology, discoveredEntity, CloudTopology::getDatabaseTier, ImmutableSet.of(
                CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE));
    }

    /**
     * Constructor.
     *
     * @param providerOid provider OID
     * @param commodityCapacities commodity capacity map
     */
    public DatabaseProviderInfo(Long providerOid, Map<Integer, Double> commodityCapacities) {
        super(providerOid, commodityCapacities, EntityType.DATABASE_VALUE);
    }
}
