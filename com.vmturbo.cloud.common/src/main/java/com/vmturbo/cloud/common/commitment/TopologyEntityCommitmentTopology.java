package com.vmturbo.cloud.common.commitment;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import org.apache.commons.collections4.SetUtils;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A {@link CloudCommitmentTopology} implementation focused on processing {@link TopologyEntityDTO} instances.
 */
public class TopologyEntityCommitmentTopology implements CloudCommitmentTopology {

    private static final Set<EntityType> COVERAGE_SUPPORTED_ENTITY_TYPES = ImmutableSet.of(
            EntityType.VIRTUAL_MACHINE);

    private static final SetMultimap<String, CloudCommitmentCoverageTypeInfo> SERVICE_PROVIDER_COVERAGE_VECTOR_MAP =
            ImmutableSetMultimap.<String, CloudCommitmentCoverageTypeInfo>builder()
                    .put("gcp", CloudCommitmentCoverageTypeInfo.newBuilder()
                            .setCoverageType(CloudCommitmentCoverageType.COMMODITY)
                            .setCoverageSubtype(CommodityType.NUM_VCORE_VALUE)
                            .build())
                    .put("gcp", CloudCommitmentCoverageTypeInfo.newBuilder()
                            .setCoverageType(CloudCommitmentCoverageType.COMMODITY)
                            .setCoverageSubtype(CommodityType.MEM_PROVISIONED_VALUE)
                            .build())
                    .build();

    private final CloudTopology<TopologyEntityDTO> cloudTopology;

    private TopologyEntityCommitmentTopology(@Nonnull CloudTopology<TopologyEntityDTO> cloudTopology) {
        this.cloudTopology = Objects.requireNonNull(cloudTopology);
    }

    @Override
    public Set<Long> getCoveredCloudServices(long commitmentId) {
        return cloudTopology.getEntity(commitmentId)
                .map(commitmentEntity -> commitmentEntity.getConnectedEntityListList()
                        .stream()
                        .filter(connectedEntity -> connectedEntity.getConnectionType() == ConnectionType.NORMAL_CONNECTION
                                && connectedEntity.getConnectedEntityType() == EntityType.CLOUD_SERVICE_VALUE)
                        .map(ConnectedEntity::getConnectedEntityId)
                        .collect(ImmutableSet.toImmutableSet()))
                .orElse(ImmutableSet.of());
    }

    @Override
    public Set<Long> getCoveredAccounts(long commitmentId) {
        return cloudTopology.getEntity(commitmentId)
                .map(commitmentEntity -> commitmentEntity.getConnectedEntityListList()
                        .stream()
                        .filter(connectedEntity -> connectedEntity.getConnectionType() == ConnectionType.NORMAL_CONNECTION
                                && connectedEntity.getConnectedEntityType() == EntityType.BUSINESS_ACCOUNT_VALUE)
                        .map(ConnectedEntity::getConnectedEntityId)
                        .collect(ImmutableSet.toImmutableSet()))
                .orElse(ImmutableSet.of());
    }

    @Override
    public double getCoverageCapacityForEntity(long entityOid, @Nonnull CloudCommitmentCoverageTypeInfo coverageTypeInfo) {
        switch (coverageTypeInfo.getCoverageType()) {
            case COUPONS:
                return cloudTopology.getRICoverageCapacityForEntity(entityOid);
            case COMMODITY:
                return cloudTopology.getEntity(entityOid)
                        .map(entity -> extractCommodityCapacity(entity, coverageTypeInfo.getCoverageSubtype()))
                        .orElse(0.0);
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported coverage type: %s", coverageTypeInfo.getCoverageType()));
        }
    }

    @Override
    public Set<CloudCommitmentCoverageTypeInfo> getSupportedCoverageVectors(long entityOid) {

        return cloudTopology.getEntity(entityOid)
                .filter(entity -> COVERAGE_SUPPORTED_ENTITY_TYPES.contains(entity.getEntityType()))
                .map(entity -> cloudTopology.getServiceProvider(entityOid)
                        .map(sp -> sp.getDisplayName().toLowerCase())
                        .map(spName -> SetUtils.emptyIfNull(SERVICE_PROVIDER_COVERAGE_VECTOR_MAP.get(spName)))
                        .orElse(Collections.emptySet()))
                .orElse(Collections.emptySet());
    }

    @Override
    public Map<CloudCommitmentCoverageTypeInfo, Double> getCommitmentCapacityVectors(long commitmentOid) {
        return cloudTopology.getEntity(commitmentOid)
                .map(commitment -> CloudCommitmentUtils.resolveCapacityVectors(commitment))
                .orElse(Collections.emptyMap());
    }

    @Override
    public boolean isSupportedAccount(long accountOid) {
        return cloudTopology.getServiceProvider(accountOid)
                .map(sp -> sp.getDisplayName().toLowerCase())
                .map(SERVICE_PROVIDER_COVERAGE_VECTOR_MAP::containsKey)
                .orElse(false);
    }

    private double extractCommodityCapacity(@Nonnull TopologyEntityDTO buyer,
                                            int commodityType) {

        final Optional<CommodityBoughtDTO> commodityBought = buyer.getCommoditiesBoughtFromProvidersList()
                .stream()
                // Right now, only compute tier resources are supported.
                .filter(commoditiesBought -> commoditiesBought.getProviderEntityType() == EntityType.COMPUTE_TIER_VALUE)
                .flatMap(commoditiesBought -> commoditiesBought.getCommodityBoughtList().stream())
                .filter(commBought -> commBought.getCommodityType().getType() == commodityType)
                .findAny();

        if (commodityBought.isPresent()) {
            return commodityBought.get().getUsed();
        } else {
            return cloudTopology.getComputeTier(buyer.getOid())
                    .map(computeTier -> computeTier.getCommoditySoldListList()
                            .stream()
                            .filter(commoditySold -> commoditySold.getCommodityType().getType() == commodityType)
                            .map(commSold -> commSold.getCapacity())
                            .findFirst()
                            .orElse(0.0))
                    .orElse(0.0);
        }
    }

    /**
     * A {@link com.vmturbo.cloud.common.commitment.CloudCommitmentTopology.CloudCommitmentTopologyFactory} implementation, which
     * created {@link TopologyEntityCommitmentTopology} instances.
     */
    public static class TopologyEntityCommitmentTopologyFactory implements CloudCommitmentTopologyFactory<TopologyEntityDTO> {

        @Override
        public CloudCommitmentTopology createTopology(@Nonnull CloudTopology<TopologyEntityDTO> cloudTopology) {
            return new TopologyEntityCommitmentTopology(cloudTopology);
        }
    }
}
