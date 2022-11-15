package com.vmturbo.repository.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import io.grpc.StatusRuntimeException;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStat;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ScopeFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ScopeFilter.EntityScope;
import com.vmturbo.common.protobuf.action.ActionDTO.GetCurrentActionStatsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetCurrentActionStatsRequest.SingleQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.GetCurrentActionStatsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse.TypeCase;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainGroupBy;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainStat;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainStat.StatGroup;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;

/**
 * Responsible for taking a {@link SupplyChain} and collecting entity counts into buckets
 * according to the provided {@link SupplyChainGroupBy} criteria.
 */
public class SupplyChainStatistician {

    private static final Logger logger = LogManager.getLogger();

    private final SupplementaryDataFactory supplementaryDataFactory;

    /**
     * This holds the mapping from a type of tier that represent the template of
     * that entity.
     */
    private static final Map<ApiEntityType, ApiEntityType> TYPE_TO_TIER = ImmutableMap.of(
            ApiEntityType.VIRTUAL_MACHINE, ApiEntityType.COMPUTE_TIER,
            ApiEntityType.DATABASE, ApiEntityType.DATABASE_TIER,
            ApiEntityType.DATABASE_SERVER, ApiEntityType.DATABASE_SERVER_TIER,
            ApiEntityType.VIRTUAL_VOLUME, ApiEntityType.STORAGE_TIER,
            ApiEntityType.VIRTUAL_MACHINE_SPEC, ApiEntityType.COMPUTE_TIER
        );

    /**
     * Create a new {@link SupplyChainStatistician}.
     *
     * @param severityService Stub to get entity severities.
     * @param actionsService Stub to get action stats.
     * @param groupService Stub to get data about groups.
     */
    public SupplyChainStatistician(final EntitySeverityServiceBlockingStub severityService,
            final ActionsServiceBlockingStub actionsService,
            final GroupServiceBlockingStub groupService) {
        this(new SupplementaryDataFactory(severityService, actionsService, groupService));
    }

    @VisibleForTesting
    SupplyChainStatistician(@Nonnull final SupplementaryDataFactory supplementaryDataFactory) {
        this.supplementaryDataFactory = supplementaryDataFactory;
    }

    /**
     * Handles the multi-plexing group-by criteria in the input list of {@link SupplyChainGroupBy}.
     *
     * <p>The multi-plexing criteria are those that can have multiple values for a single
     * {@link RepoGraphEntity}. These are things like action categories (since an entity can have
     * associated actions with multiple categories). They affect the number of stat groups that
     * a particular entity will fall into.
     *
     * @param entity The {@link RepoGraphEntity}.
     * @param groupByList The {@link SupplyChainGroupBy}.
     * @param supplementaryData The {@link SupplementaryData} to use for out-of-repository data
     *                          lookups.
     * @return One {@link StatGroupKey} for each stat group this entity will fall into, with all
     *    multi-plexing group by values set on each key.
     *    {@link SupplyChainStatistician#groupsForEntity(RepoGraphEntity, List, SupplementaryData)}
     *    is responsible for filling each key out with the other group-by values.
     */
    @Nonnull
    private List<StatGroupKey> multiplexEntityGroup(@Nonnull final RepoGraphEntity entity,
                                                    @Nonnull final List<SupplyChainGroupBy> groupByList,
                                                    @Nonnull final SupplementaryData supplementaryData) {
        // Some group-by criteria can results in multiple StatGroupKeys for a single entity.
        // These interact with each other (the final result is the cross-product).
        // Go through those criteria and create the necessary keys.
        final List<StatGroupKey> retStats = new ArrayList<>();
        if (groupByList.contains(SupplyChainGroupBy.TARGET) && groupByList.contains(SupplyChainGroupBy.ACTION_CATEGORY)) {
            final List<Long> discoveringTargets = entity.getDiscoveringTargetIds().collect(Collectors.toList());
            final Set<ActionCategory> categories = supplementaryData.getCategories(entity.getOid());
            if (discoveringTargets.isEmpty() && categories.isEmpty()) {
                retStats.add(new StatGroupKey());
            } else if (discoveringTargets.isEmpty()) {
                categories.forEach(category -> retStats.add(new StatGroupKey()
                    .setActionCategory(category)));
            } else if (categories.isEmpty()) {
                discoveringTargets.forEach(targetId -> retStats.add(new StatGroupKey()
                    .setDiscoveringTarget(targetId)));
            } else {
                discoveringTargets.forEach(targetId -> {
                    categories.forEach(category -> {
                        retStats.add(new StatGroupKey()
                            .setDiscoveringTarget(targetId)
                            .setActionCategory(category));
                    });
                });
            }
        } else if (groupByList.contains(SupplyChainGroupBy.TARGET)) {
            entity.getDiscoveringTargetIds().forEach(targetId -> {
                retStats.add(new StatGroupKey()
                    .setDiscoveringTarget(targetId));
            });
        } else if (groupByList.contains(SupplyChainGroupBy.ACTION_CATEGORY)) {
            supplementaryData.getCategories(entity.getOid())
                .forEach(category -> retStats.add(new StatGroupKey()
                    .setActionCategory(category)));
        }

        if (retStats.isEmpty()) {
            retStats.add(new StatGroupKey());
        }

        return retStats;
    }

    /**
     * Get the {@link StatGroupKey} for stat groups/buckets an entity belongs to.
     *
     * @param entity The {@link RepoGraphEntity}.
     * @param groupByList The {@link SupplyChainGroupBy}.
     * @param supplementaryData The {@link SupplementaryData} to use for out-of-repository data
     *                          lookups.
     * @return The list of {@link StatGroupKey} for the stat groups this entity falls into. This will
     *         always have at least one element.
     */
    @Nonnull
    private List<StatGroupKey> groupsForEntity(@Nonnull final RepoGraphEntity entity,
                                               @Nonnull final List<SupplyChainGroupBy> groupByList,
                                               @Nonnull final SupplementaryData supplementaryData) {
        // First we figure out the NUMBER of stat groups (affected by group by criteria that can
        // have more than one value per entity), and initialize the keys.
        final List<StatGroupKey> retStats = multiplexEntityGroup(entity, groupByList, supplementaryData);

        // Now we set the relevant group-by values for each key.
        retStats.forEach(statGroupKey -> {
            groupByList.forEach(groupBy -> {
                switch (groupBy) {
                    case ENTITY_TYPE:
                        statGroupKey.setEntityType(ApiEntityType.fromType(entity.getEntityType()));
                        break;
                    case ENTITY_STATE:
                        statGroupKey.setEntityState(entity.getEntityState());
                        break;
                    case SEVERITY:
                        final Severity severity = supplementaryData.getSeverity(entity.getOid());
                        statGroupKey.setSeverity(severity);
                        break;
                    case ACTION_CATEGORY:
                        // We handled this above when multi-plexing.
                        break;
                    case TEMPLATE:
                        getTemplate(entity).ifPresent(statGroupKey::setTemplate);
                        break;
                    case TARGET:
                        // We handled this above when multi-plexing the group-by criteria into
                        // the builders array.
                        break;
                    case RESOURCE_GROUP:
                        supplementaryData.getResourceGroupId(entity.getOid())
                                .ifPresent(statGroupKey::setResourceGroupId);
                        break;
                    case NODE_POOL:
                        supplementaryData.getNodePoolId(entity.getOid())
                                .ifPresent(statGroupKey::setNodePoolId);
                        break;
                    case BUSINESS_ACCOUNT_ID:
                        Optional<RepoGraphEntity> firstBaOwner;
                        do {
                            firstBaOwner = entity.getOwner();
                            // Traverse up ownership until we get no owner, or until we hit
                            // a business account.
                        } while (firstBaOwner.isPresent() &&
                            firstBaOwner.get().getEntityType() != ApiEntityType.BUSINESS_ACCOUNT.typeNumber());

                        if (firstBaOwner.isPresent()) {
                            statGroupKey.setOwnerBusinessAccount(firstBaOwner.get().getOid());
                        }
                        break;
                    default:
                        logger.warn("Unhandled supply chain stat group-by criteria: {}", groupBy);
                }
            });
        });
        return retStats;
    }

    @Nonnull
    private Optional<String> getTemplate(@Nonnull RepoGraphEntity entity) {
        final ApiEntityType tierType =
            TYPE_TO_TIER.get(ApiEntityType.fromType(entity.getEntityType()));

        // If there is no tier defined for this entity type just return the template type as unknown
        if (tierType == null) {
            return Optional.empty();
        }

        // Find the tier in the consumer and return its display name
        return entity.getProviders()
            .stream()
            .filter(e -> tierType.typeNumber() == e.getEntityType())
            .map(RepoGraphEntity::getDisplayName)
            .findAny();
    }

    /**
     * Utility interface to look up entities in the topology. Used to avoid passing in the
     * whole topology graph into the {@link SupplyChainStatistician}, since it doesn't really
     * need to know about where the entities are coming from.
     */
    @FunctionalInterface
    public interface TopologyEntityLookup {

        /**
         * Get a {@link RepoGraphEntity} by OID.
         *
         * @param oid The OID.
         * @return The entity, if found. Empty {@link Optional} otherwise.
         */
        @Nonnull
        Optional<RepoGraphEntity> getEntity(long oid);

    }

    /**
     * Factory class for {@link SupplementaryData}, responsible for making any required
     * RPC calls in bulk, and helpful for separately testing the related entity retrieval
     * and the code that relies on {@link SupplementaryData}.
     */
    static class SupplementaryDataFactory {

        private final EntitySeverityServiceBlockingStub severityService;

        private final ActionsServiceBlockingStub actionsService;

        private final GroupServiceBlockingStub groupService;

        @VisibleForTesting
        SupplementaryDataFactory(final EntitySeverityServiceBlockingStub severityService,
                final ActionsServiceBlockingStub actionsService,
                final GroupServiceBlockingStub groupService) {
            this.severityService = severityService;
            this.actionsService = actionsService;
            this.groupService = groupService;
        }

        private Map<Long, Set<ActionCategory>> getCategoriesById(@Nonnull final List<Long> supplyChainEntities) {
            final long queryId = 1;
            final GetCurrentActionStatsResponse response;
            try {
                response = actionsService.getCurrentActionStats(GetCurrentActionStatsRequest.newBuilder()
                    .addQueries(SingleQuery.newBuilder()
                        .setQueryId(queryId)
                        .setQuery(CurrentActionStatsQuery.newBuilder()
                            .setScopeFilter(ScopeFilter.newBuilder()
                                .setEntityList(EntityScope.newBuilder()
                                    .addAllOids(supplyChainEntities)))
                            .addGroupBy(GroupBy.TARGET_ENTITY_ID)
                            .addGroupBy(GroupBy.ACTION_CATEGORY)))
                    .build());
            } catch (StatusRuntimeException e) {
                logger.error("Failed to retrieve action categories. Error: ", e.getMessage());
                return Collections.emptyMap();
            }

            final Optional<Map<Long, Set<ActionCategory>>> actionCategoriesOpt = response.getResponsesList().stream()
                .filter(resp -> resp.getQueryId() == queryId)
                .findFirst()
                .map(singleResponse -> {
                    final Map<Long, Set<ActionCategory>> categoriesForEntity = new HashMap<>();
                    singleResponse.getActionStatsList().stream()
                        .filter(stat -> stat.getActionCount() > 0)
                        .map(CurrentActionStat::getStatGroup)
                        .forEach(statGroup -> {
                            categoriesForEntity.computeIfAbsent(statGroup.getTargetEntityId(),
                                k -> new HashSet<>()).add(statGroup.getActionCategory());
                        });
                    return categoriesForEntity;
                });
            if (!actionCategoriesOpt.isPresent()) {
                // This shouldn't happen - the action orchestrator contract says each query in
                // the request will have an associated response.
                logger.warn("Action stat query didn't return stats for the query! Response: {}",
                    response);
            }
            return actionCategoriesOpt.orElse(Collections.emptyMap());
        }

        @Nonnull
        private Map<Long, Severity> getSeveritiesById(@Nonnull final List<Long> supplyChainEntities,
                                                      @Nonnull final long topoContextId) {
            try {
                Map<Long, Severity> entityToSeverity = new HashMap<>();
                final Iterable<EntitySeveritiesResponse> resp = () ->
                    severityService.getEntitySeverities(
                    MultiEntityRequest.newBuilder()
                        .setTopologyContextId(topoContextId)
                        .addAllEntityIds(supplyChainEntities)
                        .build());
                StreamSupport.stream(resp.spliterator(), false)
                    .forEach(chunk -> {
                        if (chunk.getTypeCase() == TypeCase.ENTITY_SEVERITY) {
                            chunk.getEntitySeverity()
                                .getEntitySeverityList()
                                .forEach(entity -> entityToSeverity.put(entity.getEntityId(),
                                    entity.getSeverity()));
                        }
                    });
                return entityToSeverity;
            } catch (StatusRuntimeException e) {
                logger.error("Failed to get entity severities. Error: {}", e.getMessage());
                return Collections.emptyMap();
            }
        }

        @Nonnull
        private Map<Long, Long> getResourceGroupsById(@Nonnull final List<Long> supplyChainEntities) {
            if (supplyChainEntities.isEmpty()) {
                logger.warn("There is no supplyChain entities.");
                return Collections.emptyMap();
            }
            try {
                final GetGroupsForEntitiesResponse groupsForEntities =
                        groupService.getGroupsForEntities(GetGroupsForEntitiesRequest.newBuilder()
                                .addAllEntityId(supplyChainEntities)
                                .addGroupType(GroupType.RESOURCE)
                                .build());
                if (!groupsForEntities.getEntityGroupMap().isEmpty()) {
                    return groupsForEntities.getEntityGroupMap()
                            .entrySet()
                            .stream()
                            .filter(el -> !el.getValue().getGroupIdList().isEmpty())
                            .collect(Collectors.toMap(Entry::getKey,
                                    el -> el.getValue().getGroupIdList().get(0)));
                } else {
                    logger.trace("There is no resource groups for {} entities",
                            supplyChainEntities);
                    return Collections.emptyMap();
                }
            } catch (StatusRuntimeException e) {
                logger.error("Failed to retrieve resource groups. Error: {}", e.getMessage());
                return Collections.emptyMap();
            }
        }

        @Nonnull
        private Map<Long, Long> getNodePoolsById(@Nonnull final List<Long> supplyChainEntities) {
            if (supplyChainEntities.isEmpty()) {
                logger.warn("There is no supplyChain entities.");
                return Collections.emptyMap();
            }
            try {
                final GetGroupsForEntitiesResponse groupsForEntities =
                        groupService.getGroupsForEntities(GetGroupsForEntitiesRequest.newBuilder()
                                .addAllEntityId(supplyChainEntities)
                                .addGroupType(GroupType.NODE_POOL)
                                .build());
                if (!groupsForEntities.getEntityGroupMap().isEmpty()) {
                    return groupsForEntities.getEntityGroupMap()
                            .entrySet()
                            .stream()
                            .filter(el -> !el.getValue().getGroupIdList().isEmpty())
                            .collect(Collectors.toMap(Entry::getKey,
                                    el -> el.getValue().getGroupIdList().get(0)));
                } else {
                    logger.trace("There is no node pools for {} entities",
                            supplyChainEntities);
                    return Collections.emptyMap();
                }
            } catch (StatusRuntimeException e) {
                logger.error("Failed to retrieve node pools. Error: {}", e.getMessage());
                return Collections.emptyMap();
            }
        }

        @Nonnull
        @VisibleForTesting
        SupplementaryData newSupplementaryData(@Nonnull final List<Long> supplyChainEntities,
                                               @Nonnull final List<SupplyChainGroupBy> groupByList,
                                                @Nonnull final long topoContextId) {
            final Map<Long, Set<ActionCategory>> actionCategoriesById =
                groupByList.contains(SupplyChainGroupBy.ACTION_CATEGORY) ?
                    getCategoriesById(supplyChainEntities) :
                    Collections.emptyMap();
            final Map<Long, Severity> severitiesById =
                groupByList.contains(SupplyChainGroupBy.SEVERITY) ?
                    getSeveritiesById(supplyChainEntities, topoContextId) :
                    Collections.emptyMap();
            final Map<Long, Long> resourceGroupsById =
                groupByList.contains(SupplyChainGroupBy.RESOURCE_GROUP) ?
                    getResourceGroupsById(supplyChainEntities) :
                        Collections.emptyMap();
            final Map<Long, Long> nodePoolsById =
                    groupByList.contains(SupplyChainGroupBy.NODE_POOL) ?
                            getNodePoolsById(supplyChainEntities) :
                            Collections.emptyMap();
            return new SupplementaryData(severitiesById, actionCategoriesById, resourceGroupsById, nodePoolsById);
        }
    }

    /**
     * Some group-by criteria require data that doesn't exist in the {@link RepoGraphEntity}
     * (severities, action categories, and - in the future - resource groups). This utility class
     * allows access to that external information.
     */
    @VisibleForTesting
    @Immutable
    static class SupplementaryData {
        private final Map<Long, Severity> severitiesByEntity;
        private final Map<Long, Set<ActionCategory>> categoriesForEntity;
        private final Map<Long, Long> resourceGroupForEntity;
        private final Map<Long, Long> nodePoolForEntity;

        private SupplementaryData(final Map<Long, Severity> severitiesByEntity,
                                  final Map<Long, Set<ActionCategory>> categoriesForEntity,
                final Map<Long, Long> resourceGroupForEntity,
                                  final Map<Long, Long> nodePoolForEntity) {
            this.severitiesByEntity = severitiesByEntity;
            this.categoriesForEntity = categoriesForEntity;
            this.resourceGroupForEntity = resourceGroupForEntity;
            this.nodePoolForEntity = nodePoolForEntity;
        }

        @Nonnull
        Severity getSeverity(final long oid) {
            return severitiesByEntity.getOrDefault(oid, Severity.NORMAL);
        }

        @Nonnull
        Set<ActionCategory> getCategories(final long oid) {
            return categoriesForEntity.getOrDefault(oid, Collections.emptySet());
        }

        Optional<Long> getResourceGroupId(final long oid) {
            return Optional.ofNullable(resourceGroupForEntity.get(oid));
        }

        Optional<Long> getNodePoolId(final long oid) {
            return Optional.ofNullable(nodePoolForEntity.get(oid));
        }
    }

    /**
     * Calculate stats on a specific {@link SupplyChain}.
     *
     * @param supplyChain The {@link SupplyChain}. We will include all members of the input supply
     *                    chain in the returned stats.
     * @param groupByList The criteria to group by. This will directly affect the number of stats
     *                    returned. We look at the value of every group-by field for every entity
     *                    in the supply chain, and return a single {@link SupplyChainStat} for each
     *                    unique combination of the values. See: {@link StatGroup}.
     * @param entityLookup Interface to look up other {@link RepoGraphEntity}s in the topology.
     * @param topoContextId The topologyContextId to be queried
     * @return The list of {@link SupplyChainStat}s to return to the client.
     */
    @Nonnull
    public List<SupplyChainStat> calculateStats(@Nonnull final SupplyChain supplyChain,
                                                @Nonnull final List<SupplyChainGroupBy> groupByList,
                                                @Nonnull final TopologyEntityLookup entityLookup,
                                                @Nonnull final long topoContextId) {
        final Map<StatGroupKey, MutableLong> countsByKey = new HashMap<>();
        final Set<Long> missingEntities = new HashSet<>();

        // Using a set is unnecessary overhead, since each OID appears only once in the supply chain.
        final List<Long> supplyChainEntities = supplyChain.getSupplyChainNodesList().stream()
            .flatMap(node -> node.getMembersByStateMap().values().stream())
            .flatMap(memberList -> memberList.getMemberOidsList().stream())
            .collect(Collectors.toList());

        final SupplementaryData supplementaryData =
            supplementaryDataFactory.newSupplementaryData(supplyChainEntities, groupByList, topoContextId);


        for (Long oid : supplyChainEntities) {
            Optional<RepoGraphEntity> entity = entityLookup.getEntity(oid);
            if (entity.isPresent()) {
                final List<StatGroupKey> keys = groupsForEntity(entity.get(), groupByList, supplementaryData);
                keys.forEach(key -> countsByKey.computeIfAbsent(key, k -> new MutableLong(0))
                    .increment());
            } else {
                missingEntities.add(oid);
            }
        }

        final List<SupplyChainStat> retStats = new ArrayList<>(countsByKey.size());
        countsByKey.forEach((key, count) -> {
            retStats.add(SupplyChainStat.newBuilder()
                .setStatGroup(key.toStatGroup())
                .setNumEntities(count.getValue())
                .build());
        });


        if (missingEntities.size() > 0) {
            // This shouldn't happen, because the topology lookup we pass in should be for the
            // same topology the supply chain was calculated from.
            logger.error("Failed to find {} entities from the supply chain " +
                "in the repository. Printing IDs at debug level.", missingEntities.size());
            logger.debug("Missing entities: {}", missingEntities);
        }
        return retStats;
    }

    /**
     * The "key" for a particular group/bucket of statistics.
     * We build up the key for each supply chain entity based on the group-by criteria
     * in the input, and use it to determine which stat bucket count to increment.
     *
     * <p>This class is meant to be used for comparisons, so it should have an up-to-date
     * {@link StatGroupKey#equals(Object)} and {@link StatGroupKey#hashCode()} method.
     */
    private static class StatGroupKey {

        private ApiEntityType entityType = null;

        private EntityState entityState = null;

        private Long discoveringTarget = null;

        private Long ownerBusinessAccount = null;

        private Severity severity = null;

        private ActionCategory actionCategory = null;

        private String template = null;

        private Long resourceGroupId = null;

        private Long nodePoolId = null;

        /**
         * Convert to a {@link StatGroup} protobuf.
         *
         * @return The {@link StatGroup} that can be returned to the caller.
         */
        @Nonnull
        StatGroup toStatGroup() {
            final StatGroup.Builder retBldr = StatGroup.newBuilder();
            if (entityType != null) {
                retBldr.setEntityType(entityType.typeNumber());
            }
            if (entityState != null) {
                retBldr.setEntityState(entityState);
            }
            if (discoveringTarget != null) {
                retBldr.setTargetId(discoveringTarget);
            }
            if (ownerBusinessAccount != null) {
                retBldr.setAccountId(ownerBusinessAccount);
            }
            if (severity != null) {
                retBldr.setSeverity(severity);
            }
            if (actionCategory != null) {
                retBldr.setActionCategory(actionCategory);
            }

            if (template != null) {
                retBldr.setTemplate(template);
            }

            if (resourceGroupId != null) {
                retBldr.setResourceGroupId(resourceGroupId);
            }

            if (nodePoolId != null) {
                retBldr.setNodePoolId(nodePoolId);
            }
            return retBldr.build();
        }

        @Nonnull
        StatGroupKey setResourceGroupId(final Long resourceGroupId) {
            this.resourceGroupId = resourceGroupId;
            return this;
        }

        @Nonnull
        StatGroupKey setNodePoolId(final Long nodePoolId) {
            this.nodePoolId = nodePoolId;
            return this;
        }

        @Nonnull
        StatGroupKey setDiscoveringTarget(final Long discoveringTarget) {
            this.discoveringTarget = discoveringTarget;
            return this;
        }

        @Nonnull
        StatGroupKey setActionCategory(final ActionCategory actionCategory) {
            this.actionCategory = actionCategory;
            return this;
        }

        @Nonnull
        StatGroupKey setSeverity(final Severity severity) {
            this.severity = severity;
            return this;
        }

        @Nonnull
        StatGroupKey setOwnerBusinessAccount(final Long ownerBusinessAccount) {
            this.ownerBusinessAccount = ownerBusinessAccount;
            return this;
        }

        @Nonnull
        StatGroupKey setEntityState(final EntityState entityState) {
            this.entityState = entityState;
            return this;
        }

        @Nonnull
        StatGroupKey setEntityType(final ApiEntityType entityType) {
            this.entityType = entityType;
            return this;
        }

        @Nonnull
        StatGroupKey setTemplate(final String template) {
            this.template = template;
            return this;
        }

        @Override
        public boolean equals(Object other) {
            if (other == null) {
                return false;
            }
            if (other == this) {
                return true;
            }
            if (other instanceof StatGroupKey) {
                StatGroupKey otherKey = (StatGroupKey)other;
                return otherKey.entityType == entityType &&
                    otherKey.entityState == entityState &&
                    Objects.equals(otherKey.discoveringTarget, discoveringTarget) &&
                    Objects.equals(otherKey.ownerBusinessAccount, ownerBusinessAccount) &&
                    otherKey.severity == severity &&
                    otherKey.actionCategory == actionCategory &&
                    Objects.equals(otherKey.resourceGroupId, resourceGroupId) &&
                        Objects.equals(otherKey.nodePoolId, nodePoolId) &&
                    Objects.equals(otherKey.template, template);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(entityType, entityState, discoveringTarget,
                ownerBusinessAccount, severity, actionCategory, template, resourceGroupId, nodePoolId);
        }
    }
}
