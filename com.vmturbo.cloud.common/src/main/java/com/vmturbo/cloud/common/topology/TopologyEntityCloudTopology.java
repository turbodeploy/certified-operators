package com.vmturbo.cloud.common.topology;

import static com.vmturbo.common.protobuf.topology.TopologyDTOUtil.PRIMARY_TIER_VALUES;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VMBillingType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * A {@link CloudTopology} for {@link TopologyEntityDTO}, to be used when running the cost
 * library in the cost component.
 */
public class TopologyEntityCloudTopology implements CloudTopology<TopologyEntityDTO> {

    private static final Logger logger = LogManager.getLogger();
    private static final int MAX_SP_LOCATOR_DEPTH = 1000;
    private static final Set<Integer> SERVICE_PROVIDER =
            ImmutableSet.of(EntityType.SERVICE_PROVIDER_VALUE);
    private static final Set<Integer> REGION_AND_AVZONE =
            ImmutableSet.of(EntityType.REGION_VALUE, EntityType.AVAILABILITY_ZONE_VALUE);

    private final Map<Long, TopologyEntityDTO> topologyEntitiesById;

    private final Map<Long, Long> ownedBy;

    private final Map<Long, Set<Long>> aggregatedBy;

    private final Map<Long, Long> serviceForEntity;

    private final SetOnce<Map<Long, GroupAndMembers>> businessAccountIdToBillingFamilyGroup
            = new SetOnce<>();

    private final GroupMemberRetriever groupMemberRetriever;

    private final SetOnce<Map<Long, GroupAndMembers>> entityOidToResourceGroupOid
            = new SetOnce<>();

    private final Map<Integer, Function<TopologyEntityDTO, Optional<TopologyEntityDTO>>> spLocator =
            ImmutableMap.<Integer, Function<TopologyEntityDTO, Optional<TopologyEntityDTO>>>builder()
                    .put(EntityType.VIRTUAL_MACHINE_VALUE, v -> getAggregator(v, REGION_AND_AVZONE))
                    .put(EntityType.COMPUTE_TIER_VALUE, v -> getAggregator(v, REGION_AND_AVZONE))
                    .put(EntityType.VIRTUAL_VOLUME_VALUE, v -> getAggregator(v, REGION_AND_AVZONE))
                    .put(EntityType.DATABASE_TIER_VALUE, v -> getAggregator(v, REGION_AND_AVZONE))
                    .put(EntityType.DATABASE_SERVER_TIER_VALUE,
                            v -> getAggregator(v, REGION_AND_AVZONE))
                    .put(EntityType.DATABASE_SERVER_VALUE, v -> getAggregator(v, REGION_AND_AVZONE))
                    .put(EntityType.DATABASE_VALUE, v -> getAggregator(v, REGION_AND_AVZONE))
                    .put(EntityType.STORAGE_TIER_VALUE, v -> getAggregator(v, REGION_AND_AVZONE))
                    .put(EntityType.AVAILABILITY_ZONE_VALUE, this::getOwner)
                    .put(EntityType.APPLICATION_VALUE,
                            v -> getProviderByType(v, EntityType.VIRTUAL_MACHINE_VALUE))
                    .put(EntityType.APPLICATION_COMPONENT_VALUE,
                            v -> getProviderByType(v, EntityType.VIRTUAL_MACHINE_VALUE))
                    .put(EntityType.CLOUD_SERVICE_VALUE, this::getOwner)
                    .put(EntityType.REGION_VALUE, this::getOwner)
                    .put(EntityType.BUSINESS_ACCOUNT_VALUE, v -> getAggregator(v, SERVICE_PROVIDER))
                    .put(EntityType.SERVICE_VALUE, this::getOwner)
                    .put(EntityType.LOAD_BALANCER_VALUE,
                            v -> getProviderByType(v, EntityType.SERVICE_VALUE))
                    .put(EntityType.CLOUD_COMMITMENT_VALUE, this::getOwner)
                    .build();

    /**
     * Creates an instance of TopologyEntityCloudTopology with the provided topologyEntities and
     * billingFamilies.
     *
     * @param topologyEntities stream of TopologyEntityDTOs from which the CloudTopology is
     *                         constructed.
     * @param groupMemberRetriever service object to retrieve billing families information.
     */
    TopologyEntityCloudTopology(@Nonnull final Stream<TopologyEntityDTO> topologyEntities,
                                @Nonnull final GroupMemberRetriever groupMemberRetriever) {
        final Map<Long, TopologyEntityDTO> entitiesMap = new HashMap<>();
        final Map<Long, Long> ownedBy = new HashMap<>();
        final Map<Long, Set<Long>> aggregatedBy = new HashMap<>();
        final Map<Long, Long> connectedToService = new HashMap<>();
        topologyEntities.forEach(cloudEntity -> {
            final long id = cloudEntity.getOid();
            entitiesMap.put(id, cloudEntity);
            cloudEntity.getConnectedEntityListList().forEach(connection -> {
                if (connection.getConnectionType() == ConnectionType.AGGREGATED_BY_CONNECTION) {
                    aggregatedBy.computeIfAbsent(connection.getConnectedEntityId(), k -> new HashSet<>()).add(id);
                }
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
        this.aggregatedBy = Collections.unmodifiableMap(aggregatedBy);
        this.serviceForEntity = Collections.unmodifiableMap(connectedToService);
        this.groupMemberRetriever = groupMemberRetriever;
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
    public Optional<TopologyEntityDTO> getPrimaryTier(final long entityId) {
        final List<TopologyEntityDTO> primaryProviders = getProvidersOfTypes(entityId, PRIMARY_TIER_VALUES);
        if (primaryProviders.size() > 1) {
            logger.warn("Entity {} buying from multiple primary tiers. Choosing the first.",
                entityId);
        } else if (primaryProviders.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(primaryProviders.get(0));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Optional<TopologyEntityDTO> getComputeTier(final long entityId) {
        final List<TopologyEntityDTO> providers = getProvidersOfTypes(entityId, ImmutableSet.of(EntityType.COMPUTE_TIER_VALUE));
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
        final List<TopologyEntityDTO> providers = getProvidersOfTypes(entityId, ImmutableSet.of(EntityType.DATABASE_TIER_VALUE));
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
    public Optional<TopologyEntityDTO> getDatabaseServerTier(long entityId) {
        final List<TopologyEntityDTO> providers = getProvidersOfTypes(entityId, ImmutableSet.of(EntityType.DATABASE_SERVER_TIER_VALUE));
        if (providers.size() > 1) {
            logger.warn("Entity {} buying from multiple database server tiers. Choosing the first.",
                entityId);
        } else if (providers.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(providers.get(0));
    }

    @Nonnull
    @Override
    public Optional<TopologyEntityDTO> getStorageTier(final long entityId) {
        final List<TopologyEntityDTO> providers = getProvidersOfTypes(entityId, ImmutableSet.of(EntityType.STORAGE_TIER_VALUE));
        if (providers.size() > 1) {
            logger.warn("Entity {} buying from multiple storage tiers. Choosing the first.",
                    entityId);
        }
        return providers.isEmpty() ? Optional.empty() : Optional.of(providers.get(0));
    }

    @Override
    public Set<TopologyEntityDTO> getTierProviders(final long entityId) {
        return ImmutableSet.copyOf(getProvidersOfTypes(entityId, CLOUD_TIER_TYPES));
    }

    @Nonnull
    @Override
    public Collection<TopologyEntityDTO> getAttachedVolumes(final long entityId) {
        // Get attached volumes from bought commodities providers
        return getEntity(entityId)
                .map(entity -> entity.getCommoditiesBoughtFromProvidersList().stream()
                        .filter(commBought -> commBought.getProviderEntityType()
                                == EntityType.VIRTUAL_VOLUME.getNumber())
                        .map(CommoditiesBoughtFromProvider::getProviderId)
                        .map(this::getEntity)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toSet()))
                .orElse(Collections.emptySet());
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
                            logger.warn("Availability Zone {} (connected to by entity {}) has no region owner.",
                                    azId, entityId);
                        }
                        return regionOwner;
                    } else {
                        // Must be a region, because of the filter.
                        final Optional<TopologyEntityDTO> region = getEntity(regionOrAz.getConnectedEntityId());
                        if (!region.isPresent()) {
                            logger.warn("Entity {} connected to region {} which is not present in the topology!",
                                entityId, regionOrAz.getConnectedEntityId());
                        }
                        return region;
                    }
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toSet());

            if (connectedRegions.size() == 0) {
                logger.warn("Entity {} not connected to any regions, either directly or through availability zones!", entity.getOid());
                return Optional.empty();
            } else if (connectedRegions.size() > 1) {
                logger.warn("Entity {} connected to multiple regions: {}! Choosing the first.",
                    () -> entity.getOid(),
                    () -> connectedRegions.stream()
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
        Optional<TopologyEntityDTO> entityDTO = getEntity(entityId);
        return entityDTO.flatMap(entity -> {
            final List<TopologyEntityDTO> connectedAZs =
                    getConnectionsOfType(entityId, EntityType.AVAILABILITY_ZONE_VALUE);

            if (connectedAZs.size() == 0) {
                return Optional.empty();
            } else if (connectedAZs.size() > 1) {
                logger.warn("Entity {} connected to multiple availability zone: {}! Choosing the first.",
                    () -> entity.getOid(),
                    () -> connectedAZs.stream()
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

    @Override
    public Stream<TopologyEntityDTO> streamOwnedEntitiesOfType(long entityId, int entityType) {
        return getEntity(entityId)
                .map(entity -> entity.getConnectedEntityListList().stream()
                        .filter(connectedEntity -> connectedEntity.getConnectionType() == ConnectionType.OWNS_CONNECTION
                                && connectedEntity.getConnectedEntityType() == entityType)
                        .map(ConnectedEntity::getConnectedEntityId)
                        .map(this::getEntity)
                        .filter(Optional::isPresent)
                        .map(Optional::get))
                .orElse(Stream.empty());
    }

    @Override
    public Set<TopologyEntityDTO> getAggregated(long entityId, Set<Integer> entityTypes) {
        return aggregatedBy.getOrDefault(entityId, Collections.emptySet()).stream().map(this::getEntity)
                .flatMap(e -> e.isPresent() ? Stream.of(e.get()) : Stream.empty()).filter(s -> entityTypes.contains(s.getEntityType())).collect(Collectors.toSet());
    }

    @Override
    public Set<TopologyEntityDTO> getRegionsFromServiceProvider(long entityId) {
        Optional<TopologyEntityDTO> serviceProvider = getEntity(entityId);
        return serviceProvider.map(TopologyEntityDTO::getConnectedEntityListList).orElse(Collections.emptyList())
                .stream().filter(connectedEntity -> connectedEntity.getConnectedEntityType() == EntityType.REGION_VALUE)
                .flatMap(ce -> getEntity(ce.getConnectedEntityId()).isPresent() ? Stream.of(getEntity(ce.getConnectedEntityId()).get())
                        : Stream.empty()).collect(ImmutableSet.toImmutableSet());
    }

    @Nonnull
    @Override
    public Optional<TopologyEntityDTO> getServiceProvider(long entityId) {
        Optional<TopologyEntityDTO> entity = getEntity(entityId);
        return entity.flatMap(this::getServiceProvider);
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

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return topologyEntitiesById.size();
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    private List<TopologyEntityDTO> getProvidersOfTypes(final long entityId, final Set<Integer> types) {
        return getEntity(entityId)
            .map(entity -> entity.getCommoditiesBoughtFromProvidersList().stream()
                .filter(CommoditiesBoughtFromProvider::hasProviderEntityType)
                .filter(commBought -> hasProviderOfType(commBought, types))
                .map(CommoditiesBoughtFromProvider::getProviderId)
                .distinct()
                .map(providerId -> {
                    final Optional<TopologyEntityDTO> providerEntity = getEntity(providerId);
                    if (!providerEntity.isPresent()) {
                        logger.warn("Unable to find provider {} (type: {}) for entity {} in topology.",
                            () -> providerId,
                            () -> types.stream().map(String::valueOf).collect(Collectors.joining(",")),
                            () -> entityId);
                    }
                    return providerEntity;
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList()))
            .orElse(Collections.emptyList());
    }

    /**
     * This is needed to accommodate the changed provider type corresponding to the provider of a
     * {@link CommoditiesBoughtFromProvider} object in the context of a cloud migration plan- a compute tier provider
     * has the physical machine {@link EntityType}.
     *
     * @param commBought a {@link CommoditiesBoughtFromProvider} from which the provider type should be derived
     * @param types a group of {@link EntityType} we are attempting to match on
     * @return true if the {@link CommoditiesBoughtFromProvider} is provided by an entity corresponding to one of {@param types}
     */
    @Nonnull
    private boolean hasProviderOfType(
            @Nonnull final CommoditiesBoughtFromProvider commBought,
            @Nonnull final Set<Integer> types) {
        if (types.contains(commBought.getProviderEntityType())) {
            return true;
        }
        Optional<TopologyEntityDTO> topologyEntityDTOOptional = getEntity(commBought.getProviderId());
        if (topologyEntityDTOOptional.isPresent()) {
            return types.contains(topologyEntityDTOOptional.get().getEntityType());
        }
        return false;
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
                    final Optional<TopologyEntityDTO> connectedEntity = getEntity(connectedId);
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

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public List<TopologyEntityDTO> getAllRegions() {
        return getEntities().values()
                .stream()
                .filter(entity -> entity.getEntityType() == EntityType.REGION_VALUE)
                .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public List<TopologyEntityDTO> getAllEntitiesOfType(int entityType) {
        return getEntities().values()
                .stream()
                .filter(entity -> entity.getEntityType() == entityType)
                .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public List<TopologyEntityDTO> getAllEntitiesOfType(Set<Integer> entityTypes) {
        return getEntities().values()
                .stream()
                .filter(entity -> entityTypes.contains(entity.getEntityType()))
                .collect(Collectors.toList());

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getRICoverageCapacityForEntity(final long entityId) {
        return getEntity(entityId)
                .map(entity -> {
                    switch (entity.getEntityType()) {
                        case EntityType.VIRTUAL_MACHINE_VALUE:
                            final VMBillingType billingType = entity.getTypeSpecificInfo()
                                    .getVirtualMachine().getBillingType();

                            // capacity will only reflect the computeTier capacity, if the entity
                            // is in a state in which it can be covered by an RI.
                            final EntityState entityState = entity.getEntityState();
                            return (entityState == EntityState.POWERED_ON && billingType != VMBillingType.BIDDING) ?
                                    getComputeTier(entity.getOid())
                                            .map(computeTier -> computeTier.getTypeSpecificInfo()
                                                    .getComputeTier().getNumCoupons())
                                            .orElse(0D) : 0D;
                        default:
                            // if unsupported type, capacity is assumed to be 0
                            return 0D;
                    }
                }).orElse(0D);
    }

    /**
     * Returns the billing family group of the entity with the provided id.
     *
     * @param entityId of the entity for which billing family group is being returned.
     * @return billing family group of the entity with the provided id.
     */
    @Override
    @Nonnull
    public Optional<GroupAndMembers> getBillingFamilyForEntity(final long entityId) {
        businessAccountIdToBillingFamilyGroup
                .ensureSet(this::createAccountIdToBillingFamilyGroupMap);

        Optional<TopologyEntityDTO> entityDTO = getEntity(entityId);
        if (entityDTO.map(entity -> entity.getEntityType()
                == EntityType.BUSINESS_ACCOUNT_VALUE).orElse(false)) {
            return businessAccountIdToBillingFamilyGroup.getValue()
                    .map(map -> map.get(entityId));
        } else {
            if (!entityDTO.isPresent()) {
                logger.warn("Entity not found for entityId {}, unable to find billing family.", entityId);
            }
            final Long accountId = ownedBy.get(entityId);
            if (accountId == null) {
                logger.warn("OwnedBy account id not found for entityId: {}", entityId);
                return Optional.empty();
            }
            return businessAccountIdToBillingFamilyGroup.getValue()
                    .map(map -> map.get(accountId));
        }
    }

    @Override
    public boolean isSimulated() {
        return false;
    }

    /**
     * Creates a map from account id to Billing family group. It first retrieves all the billing
     * family groups from the group component and then constructs the map from account id to
     * billing family group.
     *
     * @return map from account id to billing family group.
     */
    private Map<Long, GroupAndMembers> createAccountIdToBillingFamilyGroupMap() {
        // Retrieve Billing family groups from GroupMemberRetriever
        final Collection<GroupAndMembers> billingFamilyGroups = retrieveBillingFamilyGroups();

        if (billingFamilyGroups.isEmpty()) {
            logger.warn("Received no billing family groups from the group member retriever.");
        }

        // Create map from account id to Billing Family group
        final Map<Long, GroupAndMembers> billingFamilyGroupByBusinessAccountId =
                new HashMap<>();
        billingFamilyGroups.forEach(group -> group.members()
                .forEach(id -> billingFamilyGroupByBusinessAccountId.put(id, group)));

        logger.debug("Created billing family reference map: {}",
                billingFamilyGroupByBusinessAccountId);
        return billingFamilyGroupByBusinessAccountId;
    }

    private Collection<GroupAndMembers> retrieveBillingFamilyGroups() {
        return groupMemberRetriever
                .getGroupsWithMembers(GetGroupsRequest.newBuilder()
                        .setGroupFilter(GroupFilter.newBuilder()
                                .setGroupType(GroupType.BILLING_FAMILY)
                                .build())
                        .build());
    }

    @Override
    @Nonnull
    public Optional<GroupAndMembers> getResourceGroup(long entityId) {
        entityOidToResourceGroupOid.ensureSet(this::createEntityIdToResourceGroupMap);
        return entityOidToResourceGroupOid.getValue().map(map -> map.get(entityId));
    }

    private Map<Long, GroupAndMembers> createEntityIdToResourceGroupMap() {
        // Retrieve resource groups from GroupMemberRetriever
        final Collection<GroupAndMembers> resourceGroups = retrieveResourceGroups();

        if (resourceGroups.isEmpty()) {
            logger.warn("Received no resource groups from the group member retriever.");
        }

        // Create map from entity id to resource group
        final Map<Long, GroupAndMembers> ResourceGroupByEntityId = new HashMap<>();
        resourceGroups.forEach(group -> group.members()
                .forEach(id -> ResourceGroupByEntityId.put(id, group)));

        logger.debug("Created resource group reference map: {}", ResourceGroupByEntityId);
        return ResourceGroupByEntityId;
    }

    private Collection<GroupAndMembers> retrieveResourceGroups() {
        return groupMemberRetriever
                .getGroupsWithMembers(GetGroupsRequest.newBuilder()
                        .setGroupFilter(GroupFilter.newBuilder()
                                .setGroupType(GroupType.RESOURCE)
                                .build())
                        .build());
    }

    /**
     * Get aggregator of provided entity. Aggregator is the entity connected to provided entity by
     * {@link ConnectionType#AGGREGATED_BY_CONNECTION}
     *
     * @param entity the entity
     * @param aggregatorTypeFilter include only aggregators of given types
     * @return aggregator of provided entity
     */
    @Nonnull
    public Optional<TopologyEntityDTO> getAggregator(@Nonnull TopologyEntityDTO entity,
            @Nonnull Set<Integer> aggregatorTypeFilter) {
        return entity.getConnectedEntityListList()
                .stream()
                .filter(e -> e.getConnectionType() == ConnectionType.AGGREGATED_BY_CONNECTION &&
                        aggregatorTypeFilter.contains(e.getConnectedEntityType()))
                // verify the connected entity exists prior to choosing one
                .map(e -> getEntity(e.getConnectedEntityId()))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    /**
     * Get owner for provided entity. Owner is the entity that has 'owns' connection with provided
     * entity.
     *
     * @param entity the entity
     * @return owner for provided entity.
     */
    @Nonnull
    public Optional<TopologyEntityDTO> getOwner(@Nonnull TopologyEntityDTO entity) {
        return getOwner(entity.getOid());
    }

    /**
     * Get entity provider of the given type. Entity provide is the entity from which it buys
     * commodities.
     *
     * @param entity     the entity
     * @param entityType provider entity type
     * @return entity provider of the given type.
     */
    @Nonnull
    public Optional<TopologyEntityDTO> getProviderByType(@Nonnull TopologyEntityDTO entity,
            int entityType) {
        return entity.getCommoditiesBoughtFromProvidersList()
                .stream()
                .filter(c -> c.getProviderEntityType() == entityType)
                .findFirst()
                .flatMap(c -> getEntity(c.getProviderId()));
    }

    /**
     * Get ServiceProvider for provided entity.
     *
     * @param entity the entity
     * @return ServiceProvider
     */
    @Nonnull
    public Optional<TopologyEntityDTO> getServiceProvider(@Nonnull TopologyEntityDTO entity) {
        Optional<TopologyEntityDTO> current = Optional.of(entity);
        int c = 0;
        while (current.isPresent() &&
                current.get().getEntityType() != EntityType.SERVICE_PROVIDER_VALUE) {
            final Function<TopologyEntityDTO, Optional<TopologyEntityDTO>> extractor =
                    spLocator.get(current.get().getEntityType());
            if (extractor != null) {
                current = extractor.apply(current.get());
            } else {
                logger.error("Cannot find ServiceProvider extractor for EntityType: {}",
                        entity.getEntityType());
                return Optional.empty();
            }
            if (c++ >= MAX_SP_LOCATOR_DEPTH) {
                logger.error("Cannot get ServiceProvider extractor for entity: {}." +
                        " Iteration depth exceeded.", entity.getOid());
                return Optional.empty();
            }
        }
        return current;
    }

}
