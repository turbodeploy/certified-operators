package com.vmturbo.cost.calculation.topology;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
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

    private final Map<Long, Long> ownedBy;

    private final Map<Long, Long> serviceForEntity;

    /**
     * Do not call directly. Use {@link TopologyEntityCloudTopology#newFactory()}.
     */
    private TopologyEntityCloudTopology(@Nonnull final Map<Long, TopologyEntityDTO> topologyEntitiesById) {
        this.topologyEntitiesById =
                Collections.unmodifiableMap(Objects.requireNonNull(topologyEntitiesById));
        final Map<Long, Long> ownedBy = new HashMap<>();
        final Map<Long, Long> connectedToService = new HashMap<>();
        topologyEntitiesById.forEach((id, entity) -> {
            entity.getConnectedEntityListList().forEach(connection -> {
                if (connection.getConnectionType() == ConnectionType.OWNS_CONNECTION) {
                    // We assume that an entity has at most one direct owner.
                    final Long oldOwner = ownedBy.put(connection.getConnectedEntityId(), id);
                    if (oldOwner != null) {
                        logger.error("Entity {} owned by more than one entity! " +
                                "Previous owner: {} (type {}). New owner: {} (type {})",
                                connection.getConnectedEntityId(), oldOwner,
                                topologyEntitiesById.get(oldOwner).getEntityType(),
                                id, entity.getEntityType());
                    }

                    if (entity.getEntityType() == EntityType.CLOUD_SERVICE_VALUE) {
                        connectedToService.put(connection.getConnectedEntityId(), id);
                    }
                }
            });
        });
        this.ownedBy = Collections.unmodifiableMap(ownedBy);
        this.serviceForEntity = Collections.unmodifiableMap(connectedToService);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Optional<TopologyEntityDTO> getEntity(final long entityId) {
        return Optional.ofNullable(topologyEntitiesById.get(entityId));
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Optional<TopologyEntityDTO> getConnectedRegion(final long entityId) {
        return getEntity(entityId).flatMap(entity -> {
            final Set<TopologyEntityDTO> connectedRegions = entity.getConnectedEntityListList().stream()
                .filter(connEntity -> connEntity.getConnectedEntityType() == EntityType.AVAILABILITY_ZONE_VALUE ||
                    connEntity.getConnectedEntityType() == EntityType.REGION_VALUE)
                .map(regionOrAz -> {
                    if (regionOrAz.getConnectedEntityType() == EntityType.AVAILABILITY_ZONE_VALUE) {
                        final long azId = regionOrAz.getConnectedEntityId();
                        final Optional<TopologyEntityDTO> regionOwner = getOwner(azId);
                        if (!regionOwner.isPresent()) {
                            logger.error("Availability Zone {} (connected to by entity {}) has no region owner.",
                                    azId, entityId);
                        }
                        return regionOwner;
                    } else {
                        // Must be a region, because of the filter.
                        final Optional<TopologyEntityDTO> region = getEntity(regionOrAz.getConnectedEntityId());
                        if (!region.isPresent()) {
                            logger.error("Entity {} connected to region {} which is not present in the topology!",
                                entityId, regionOrAz.getConnectedEntityId());
                        }
                        return region;
                    }
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toSet());

            if (connectedRegions.size() == 0) {
                logger.error("Entity {} not connected to any regions, either directly or through availability zones!", entity.getOid());
                return Optional.empty();
            } else if (connectedRegions.size() > 1) {
                logger.warn("Entity {} connected to multiple regions: {}! Choosing the first.",
                    connectedRegions.stream()
                        .map(region -> Long.toString(region.getOid()))
                        .collect(Collectors.joining(",")));
            }
            return Optional.of(connectedRegions.iterator().next());
        });
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<TopologyEntityDTO> getOwner(final long entityId) {
        return Optional.ofNullable(ownedBy.get(entityId))
            .flatMap(this::getEntity);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<TopologyEntityDTO> getConnectedService(final long tierId) {
        return Optional.ofNullable(serviceForEntity.get(tierId))
            .flatMap(this::getEntity);
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
