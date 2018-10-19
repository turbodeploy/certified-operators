package com.vmturbo.cost.calculation.topology;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A {@link CloudTopology} for {@link TopologyEntityDTO}, to be used when running the cost
 * library in the cost component.
 */
public class TopologyEntityCloudTopology implements CloudTopology<TopologyEntityDTO> {

    private static final Logger logger = LogManager.getLogger();

    private final Map<Long, TopologyEntityDTO> topologyEntitiesById;

    private final Map<Long, Long> ownedBy;

    private final Map<Long, Long> serviceForEntity;

    /**
     * Do not call directly, except in tests. Use {@link TopologyEntityCloudTopology}.
     */
    TopologyEntityCloudTopology(@Nonnull final Stream<TopologyEntityDTO> topologyEntitiesById) {
        final Map<Long, TopologyEntityDTO> entitiesMap = new HashMap<>();
        final Map<Long, Long> ownedBy = new HashMap<>();
        final Map<Long, Long> connectedToService = new HashMap<>();
        topologyEntitiesById.forEach(cloudEntity -> {
            final long id = cloudEntity.getOid();
            entitiesMap.put(id, cloudEntity);
            cloudEntity.getConnectedEntityListList().forEach(connection -> {
                if (connection.getConnectionType() == ConnectionType.OWNS_CONNECTION) {
                    // We assume that an entity has at most one direct owner.
                    final Long oldOwner = ownedBy.put(connection.getConnectedEntityId(), id);
                    if (oldOwner != null) {
                        logger.error("Entity {} owned by more than one entity! " +
                                        "Previous owner: {}.  New owner: {} (type {})",
                                connection.getConnectedEntityId(), oldOwner,
                                id, cloudEntity.getEntityType());
                    }

                    if (cloudEntity.getEntityType() == EntityType.CLOUD_SERVICE_VALUE) {
                        connectedToService.put(connection.getConnectedEntityId(), id);
                    }
                }
            });
        });
        this.topologyEntitiesById = Collections.unmodifiableMap(entitiesMap);
        this.ownedBy = Collections.unmodifiableMap(ownedBy);
        this.serviceForEntity = Collections.unmodifiableMap(connectedToService);
    }

    @Nonnull
    @Override
    public Map<Long, TopologyEntityDTO> getEntities() {
        return Collections.unmodifiableMap(topologyEntitiesById);
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
        final List<TopologyEntityDTO> providers = getProvidersOfType(entityId, EntityType.COMPUTE_TIER_VALUE);
        if (providers.size() > 1) {
            logger.warn("Entity {} buying from multiple compute tiers. Choosing the first.",
                    entityId);
        } else if (providers.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(providers.get(0));

    }

    @Nonnull
    @Override
    public Optional<TopologyEntityDTO> getDatabaseTier(long entityId) {
        final List<TopologyEntityDTO> providers = getProvidersOfType(entityId, EntityType.DATABASE_TIER_VALUE);
        if (providers.size() > 1) {
            logger.warn("Entity {} buying from multiple database tiers. Choosing the first.",
                    entityId);
        } else if (providers.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(providers.get(0));
    }

    @Nonnull
    @Override
    public Optional<TopologyEntityDTO> getStorageTier(final long entityId) {
        final List<TopologyEntityDTO> providers = getProvidersOfType(entityId, EntityType.STORAGE_TIER_VALUE);
        if (providers.size() > 1) {
            logger.warn("Entity {} buying from multiple storage tiers. Choosing the first.",
                    entityId);
        } else if (providers.isEmpty()) {
            final List<TopologyEntityDTO> connections =
                    getConnectionsOfType(entityId, EntityType.STORAGE_TIER_VALUE);
            if (connections.isEmpty()) {
                return Optional.empty();
            } else {
                if (connections.size() > 1) {
                    logger.warn("Entity {} connected to multiple storage tiers. Choosing the first.",
                            entityId);
                }
                return Optional.of(connections.get(0));
            }
        }
        return Optional.of(providers.get(0));
    }

    @Nonnull
    @Override
    public Collection<TopologyEntityDTO> getConnectedVolumes(final long entityId) {
        return getConnectionsOfType(entityId, EntityType.VIRTUAL_VOLUME_VALUE);
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
    @Override
    @Nonnull
    public Optional<TopologyEntityDTO> getConnectedAvailabilityZone(final long entityId) {
        return getEntity(entityId).flatMap(entity -> {
            final List<TopologyEntityDTO> connectedAZs =
                    getConnectionsOfType(entityId, EntityType.AVAILABILITY_ZONE_VALUE);

            if (connectedAZs.size() == 0) {
                logger.warn("Entity {} not connected to any availability zone!", entity.getOid());
                return Optional.empty();
            } else if (connectedAZs.size() > 1) {
                logger.warn("Entity {} connected to multiple availability zone: {}! Choosing the first.",
                        connectedAZs.stream()
                                .map(region -> Long.toString(region.getOid()))
                                .collect(Collectors.joining(",")));
            }
            return Optional.of(connectedAZs.iterator().next());
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
    public Optional<TopologyEntityDTO> getConnectedService(final long entityId) {
        return getEntity(entityId)
            .flatMap(targetEntity -> {
                if (targetEntity.getEntityType() == EntityType.CLOUD_SERVICE_VALUE) {
                    return Optional.of(targetEntity);
                } else {
                    return Optional.ofNullable(serviceForEntity.get(entityId))
                        .flatMap(this::getEntity);
                }
            });
    }

    @Override
    public int size() {
        return topologyEntitiesById.size();
    }

    @Nonnull
    private List<TopologyEntityDTO> getProvidersOfType(final long entityId, final int type) {
        return getEntity(entityId)
            .map(entity -> entity.getCommoditiesBoughtFromProvidersList().stream()
                .filter(CommoditiesBoughtFromProvider::hasProviderEntityType)
                .filter(commBought -> commBought.getProviderEntityType() == type)
                .map(CommoditiesBoughtFromProvider::getProviderId)
                .distinct()
                .map(providerId -> {
                    final Optional<TopologyEntityDTO> providerEntity = getEntity(providerId);
                    if (!providerEntity.isPresent()) {
                        logger.warn("Unable to find provider {} (type: {}) for entity {} in topology.",
                                providerId, type, entityId);
                    }
                    return providerEntity;
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList()))
            .orElse(Collections.emptyList());
    }

    @Nonnull
    private List<TopologyEntityDTO> getConnectionsOfType(final long entityId, final int type) {
        return getEntity(entityId)
            .map(entity -> entity.getConnectedEntityListList().stream()
                .filter(ConnectedEntity::hasConnectedEntityType)
                .filter(connection -> connection.getConnectedEntityType() == type)
                .map(ConnectedEntity::getConnectedEntityId)
                .distinct()
                .map(connectedId -> {
                    final Optional<TopologyEntityDTO> connectedEntity =
                            getEntity(connectedId);
                    if (!connectedEntity.isPresent()) {
                        logger.warn("Unable to find connection {} (type: {}) for entity {} in topology.",
                                connectedId, type, entityId);
                    }
                    return connectedEntity;
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList()))
            .orElse(Collections.emptyList());
    }
}
