package com.vmturbo.cost.component.savings.temold;

import java.util.Map;
import java.util.Objects;

import com.google.common.collect.ImmutableSet;
import com.google.gson.annotations.SerializedName;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DeploymentType;

/**
 * Provider Info for the Database Server entity type.
 */
public class DatabaseServerProviderInfo extends BaseProviderInfo {
    @SerializedName(value = "dt", alternate = "deploymentType")
    private DeploymentType deploymentType;

    /**
     * Constructor.
     *
     * @param cloudTopology cloud topology
     * @param discoveredEntity discovered entity
     */
    public DatabaseServerProviderInfo(CloudTopology<TopologyEntityDTO> cloudTopology,
            TopologyEntityDTO discoveredEntity) {
        super(cloudTopology, discoveredEntity, CloudTopology::getDatabaseServerTier, ImmutableSet.of(
                CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE));
        cloudTopology.getEntity(discoveredEntity.getOid()).ifPresent(entity -> deploymentType =
                entity.getTypeSpecificInfo().getDatabase().getDeploymentType());
    }

    /**
     * Constructor.
     *
     * @param providerOid provider OID
     * @param commodityCapacities commodity capacities
     * @param deploymentType deployment type
     */
    public DatabaseServerProviderInfo(Long providerOid, Map<Integer, Double> commodityCapacities,
            DeploymentType deploymentType) {
        super(providerOid, commodityCapacities, EntityType.DATABASE_SERVER_VALUE);
        this.deploymentType = deploymentType;
    }

    /**
     * Get the deployment type.
     *
     * @return deployment type
     */
    public DeploymentType getDeploymentType() {
        return deploymentType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        DatabaseServerProviderInfo that = (DatabaseServerProviderInfo)o;
        return deploymentType == that.deploymentType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), deploymentType);
    }

    @Override
    public String toString() {
        return "DatabaseServerProviderInfo{" + "providerOid=" + providerOid
                + ", commodityCapacities=" + commodityCapacities + ", entityType=" + entityType
                + ", deploymentType=" + deploymentType + '}';
    }
}
