package com.vmturbo.cost.calculation.topology;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A {@link CloudTopology} for {@link TopologyEntityDTO}, to be used when running the cost
 * library in the cost component.
 *
 * TODO (roman, Aug 16 2018): Move this to the cost component. It's provided here for illustration
 * purposes.
 */
public class TopologyEntityCloudTopology implements CloudTopology<TopologyEntityDTO> {

    private static final Logger logger = LogManager.getLogger();

    private final Map<Long, TopologyEntityDTO> topologyEntitiesById;

    /**
     * Do not call directly. Use {@link TopologyEntityCloudTopology#newFactory()}.
     */
    private TopologyEntityCloudTopology(@Nonnull final Map<Long, TopologyEntityDTO> topologyEntitiesById) {
        this.topologyEntitiesById =
                Collections.unmodifiableMap(Objects.requireNonNull(topologyEntitiesById));
    }

    @Override
    @Nonnull
    public Optional<TopologyEntityDTO> getEntity(final long entityId) {
        return Optional.ofNullable(topologyEntitiesById.get(entityId));
    }

    @Override
    @Nonnull
    public Optional<TopologyEntityDTO> getComputeTier(final long entityId) {
        return getEntity(entityId)
            .flatMap(entity -> {
                final List<CommoditiesBoughtFromProvider> boughtFromComputeTier =
                        entity.getCommoditiesBoughtFromProvidersList().stream()
                                .filter(CommoditiesBoughtFromProvider::hasProviderEntityType)
                                .filter(commBought -> commBought.getProviderEntityType() == EntityType.COMPUTE_TIER_VALUE)
                                .collect(Collectors.toList());

                if (boughtFromComputeTier.size() == 0) {
                    // This shouldn't happen with a cloud VM.
                    return Optional.empty();
                } else if (boughtFromComputeTier.size() > 1) {
                    logger.warn("Buying from multiple compute tiers. Wut?");
                }

                final CommoditiesBoughtFromProvider computeTierBought = boughtFromComputeTier.get(0);
                return getEntity(computeTierBought.getProviderId());
            });

    }

    @Override
    @Nonnull
    public Optional<TopologyEntityDTO> getRegion(final long entityId) {
        return getEntity(entityId).flatMap(entity -> {
            final Map<Integer, List<ConnectedEntity>> connectedRegionsAndAz = entity.getConnectedEntityListList().stream()
                    .filter(connEntity -> connEntity.getConnectedEntityType() == EntityType.REGION_VALUE ||
                            connEntity.getConnectedEntityType() == EntityType.AVAILABILITY_ZONE_VALUE)
                    .collect(Collectors.groupingBy(ConnectedEntity::getConnectedEntityType));

            final Set<Long> connectedRegionIds = new HashSet<>();
            final List<ConnectedEntity> connectedAZ = connectedRegionsAndAz.get(EntityType.AVAILABILITY_ZONE_VALUE);
            if (connectedAZ != null) {
                connectedAZ.stream()
                        .flatMap(availZoneConnection ->
                                getEntity(availZoneConnection.getConnectedEntityId())
                                        .map(az -> az.getConnectedEntityListList().stream()
                                                .filter(connectedEntity -> connectedEntity.getConnectedEntityType() == EntityType.REGION_VALUE)
                                                .map(ConnectedEntity::getConnectedEntityId))
                                        .orElseGet(() -> {
                                            logger.error("Entity {} connected to Availability Zone {} which is not found in topology!",
                                                    entity.getOid(), availZoneConnection.getConnectedEntityId());
                                            return Stream.empty();
                                        }))
                        .forEach(connectedRegionIds::add);
            }
            final List<ConnectedEntity> connectedRegions = connectedRegionsAndAz.get(EntityType.REGION_VALUE);
            if (connectedRegions != null) {
                connectedRegions.forEach(connectedEntity -> connectedRegionIds.add(connectedEntity.getConnectedEntityId()));
            }

            if (connectedRegionIds.size() == 0) {
                logger.error("Entity {} not connected to any regions, either directly or through availability zones!", entity.getOid());
                return Optional.empty();
            } else if (connectedRegionIds.size() > 1) {
                logger.warn("Entity {} connected to multiple regions: {}! Choosing the first.", connectedRegionIds);
            }
            final long connectedRegionId = connectedRegionIds.iterator().next();
            return getEntity(connectedRegionId);
        });
    }

    /**
     * Use this method to get a production {@link TopologyEntityCloudTopologyFactory}.
     *
     * @return A {@link TopologyEntityCloudTopologyFactory} for use in production code.
     */
    @Nonnull
    public static TopologyEntityCloudTopologyFactory newFactory() {
        return TopologyEntityCloudTopology::new;
    }

    /**
     * A factory for {@link TopologyEntityCloudTopology}, mainly for unit testing purposes.
     */
    @FunctionalInterface
    public interface TopologyEntityCloudTopologyFactory {
        @Nonnull
        TopologyEntityCloudTopology newCloudTopology(@Nonnull final Map<Long, TopologyEntityDTO> entities);
    }
}
