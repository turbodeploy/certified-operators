package com.vmturbo.api.component.external.api.mapper;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import io.grpc.StatusRuntimeException;
import io.netty.util.collection.LongObjectHashMap;
import io.netty.util.collection.LongObjectMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.util.DefaultCloudGroup;
import com.vmturbo.api.component.external.api.util.DefaultCloudGroupProducer;
import com.vmturbo.api.component.external.api.util.MagicScopeGateway;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.UserScopeUtils;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.plan.PlanDTO.OptionalPlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.repository.api.RepositoryListener;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorException;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Mapper to convert string UUID's to OID's that make sense in the
 * XL system. This class, in addition to {@link ApiId}, should encapsulate
 * all the weird constants, corner-cases, and magic strings involved in dealing
 * with the UI requests.
 */
public class UuidMapper implements RepositoryListener {

    private static final Logger logger = LogManager.getLogger();

    /**
     * In the UI, the "Market" identifies the real-time topology.
     */
    public static final String UI_REAL_TIME_MARKET_STR = "Market";

    public static final Map<String, DefaultCloudGroup> CLOUD_GROUPS_BY_UUID =
        DefaultCloudGroupProducer.getDefaultCloudGroup().stream()
            .collect(Collectors.toMap(DefaultCloudGroup::getUuid, Function.identity()));

    private final long realtimeContextId;

    private final RepositoryApi repositoryApi;

    private final TopologyProcessor topologyProcessor;

    private final MagicScopeGateway magicScopeGateway;

    private final ThinTargetCache thinTargetCache;

    private final CloudTypeMapper cloudTypeMapper;

    private final ApiIdResolver apiIdResolver;

    /**
     * We cache the {@link ApiId}s associated with specific OIDs, so that we can save the
     * information about each ID and avoid extra RPCs to determine whether type the ID refers to.
     *
     * <p>We don't expect this map to be huge, because most entities (probably) aren't going to be
     * addressed by ID.</p>
     *
     * <p>This cache cleared when new topology is available in repository.</p>
     */
    private final Map<Long, ApiId> cachedIds = Collections.synchronizedMap(new HashMap<>());

    public UuidMapper(final long realtimeContextId,
                      @Nonnull final MagicScopeGateway magicScopeGateway,
                      @Nonnull final RepositoryApi repositoryApi,
                      @Nonnull final TopologyProcessor topologyProcessor,
                      @Nonnull final PlanServiceBlockingStub planServiceBlockingStub,
                      @Nonnull final GroupMemberRetriever groupMemberRetriever,
                      @Nonnull final ThinTargetCache thinTargetCache,
                      @Nonnull final CloudTypeMapper cloudTypeMapper) {
        this.realtimeContextId = realtimeContextId;
        this.magicScopeGateway = magicScopeGateway;
        this.repositoryApi = repositoryApi;
        this.topologyProcessor = topologyProcessor;
        this.thinTargetCache = thinTargetCache;
        this.cloudTypeMapper = cloudTypeMapper;
        this.apiIdResolver = new ApiIdResolver(thinTargetCache, repositoryApi,
                groupMemberRetriever, planServiceBlockingStub);
    }

    /**
     * @param uuid The string UUID from the API.
     * @return An {@link ApiId} for the UUID.
     * @throws OperationFailedException If one of the underlying operations required to map the UUID
     *         to an {@link ApiId} fails.
     */
    @Nonnull
    public ApiId fromUuid(@Nonnull final String uuid) throws OperationFailedException {

        final String demystifiedUuid = magicScopeGateway.enter(uuid);

        final boolean isRealtime = demystifiedUuid.equals(UI_REAL_TIME_MARKET_STR) ||
            CLOUD_GROUPS_BY_UUID.containsKey(demystifiedUuid);
        final long oid = isRealtime ? realtimeContextId : Long.valueOf(demystifiedUuid);
        return cachedIds.compute(oid, (k, existing) -> {
            if (existing == null) {
                Metrics.CACHE_MISS_COUNT.increment();
                return new ApiId(oid, realtimeContextId, apiIdResolver, repositoryApi, topologyProcessor,
                        thinTargetCache, cloudTypeMapper);
            } else {
                Metrics.CACHE_HIT_COUNT.increment();
                return existing;
            }
        });
    }

    @Nonnull
    public ApiId fromOid(final long oid) {
        return cachedIds.compute(oid, (k, existing) -> {
            if (existing == null) {
                Metrics.CACHE_MISS_COUNT.increment();
                return new ApiId(oid, realtimeContextId, apiIdResolver, repositoryApi, topologyProcessor,
                        thinTargetCache, cloudTypeMapper);
            } else {
                Metrics.CACHE_HIT_COUNT.increment();
                return existing;
            }
        });
    }

    public static boolean isRealtimeMarket(String uuid) {
        return uuid.equals(UI_REAL_TIME_MARKET_STR);
    }

    @Override
    public void onSourceTopologyAvailable(long topologyId, long topologyContextId) {
        // clean cache if received realtime topology
        if (topologyContextId == realtimeContextId) {
            logger.info("Clear all cached {@link ApiId}'s associated with specific OID when new "
                + "topology (topologyId - {}) received ", topologyId);
            cachedIds.clear();
        }
    }

    /**
     * Detect whether this is a global or scoped UUID list. If there are any seed UUIDs,
     * and none of those seeds are "Market", then this is a limited scope.
     * In other words, if there are no seeds, or any of the seeds are "Market", then this is
     * _not_ a limited scope.
     *
     * @param seedUuids the set of seedUuids to define the scope
     * @return true iff there are either more than one seed uuids, or a single seed UUID
     * that is not equal to the distinguished live market UUID "Market"
     */
    public static boolean hasLimitedScope(@Nullable final Collection<String> seedUuids) {
        return !CollectionUtils.isEmpty(seedUuids) && !seedUuids.contains(UI_REAL_TIME_MARKET_STR);
    }

    /**
     * Resolve whether or not the input set of IDs refer to entities, and load any necessary
     * cached information. Note - if they are NOT entities,
     * you still won't know what they are after this call.
     *
     * @param values The input IDs.
     */
    public void bulkResolveEntities(Collection<ApiId> values) {
        apiIdResolver.bulkResolveEntities(values);
    }


    /**
     * Resolve whether or not the input set of IDs refer to groups, and load any necessary
     * cached information. Note - if they are NOT groups,
     * you still won't know what they are after this call.
     *
     * @param values The input IDs.
     */
    public void bulkResolveGroups(Collection<ApiId> values) {
        apiIdResolver.bulkResolveGroups(values);
    }

    /**
     * Utility class for bulk resolution of {@link ApiId}s, intended to minimize RPC calls when
     * a collection of IDs needs to be expanded/checked.
     */
    static class ApiIdResolver {

        private final ThinTargetCache thinTargetCache;
        private final RepositoryApi repositoryApi;
        private final GroupMemberRetriever groupMemberRetriever;
        private final PlanServiceBlockingStub planServiceBlockingStub;

        ApiIdResolver(ThinTargetCache thinTargetCache, RepositoryApi repositoryApi,
                GroupMemberRetriever groupMemberRetriever,
                PlanServiceBlockingStub planServiceBlockingStub) {
            this.thinTargetCache = thinTargetCache;
            this.repositoryApi = repositoryApi;
            this.groupMemberRetriever = groupMemberRetriever;
            this.planServiceBlockingStub = planServiceBlockingStub;
        }

        /**
         * Resolve a collection of {@link ApiId}s. Resolve means determine what type of object each
         * id refers to, and initialize the cached information for that id, using the minimum
         * number of RPC calls.
         *
         * @param apiIds The collection of {@link ApiId}s.
         */
        public void bulkResolve(Collection<ApiId> apiIds) {
            // We can use the thin target cache to avoid RPCs, so even though we are not likely
            // to see targets this is a very quick check.
            apiIds = bulkResolveTargets(apiIds);
            // Entities are quick to resolve because it's an in-memory lookup in the repository.
            apiIds = bulkResolveEntities(apiIds);
            apiIds = bulkResolveGroups(apiIds);
            // Least likely
            bulkResolvePlans(apiIds);
        }

        /**
         * Resolve a collection of {@link ApiId}s if they refer to entities. Resolve means
         * determine if the id refers to an entity and, if so, initialized the cached entity-related
         * information for the id.
         *
         * @param apiIds The {@link ApiId}s to resolve.
         * @return A collection of all non-entity {@link ApiId}s in the input.
         */
        private Collection<ApiId> bulkResolveEntities(Collection<ApiId> apiIds) {
            Set<Long> src = Collections.emptySet();
            Set<Long> projected = Collections.emptySet();
            final LongObjectMap<ApiId> map = new LongObjectHashMap<>();
            for (ApiId apiId : apiIds) {
                if (apiId.needsResolution()) {
                    map.put(apiId.oid(), apiId);
                    // Querying projected entities is slower, so we populated "projected" only
                    // with entities that are unique to the projected topology (i.e. clones).
                    // Their OIDs are negative.
                    if (apiId.oid() >= 0) {
                        if (src.isEmpty()) {
                            src = new HashSet<>();
                        }
                        src.add(apiId.oid());
                    } else {
                        if (projected.isEmpty()) {
                            projected = new HashSet<>();
                        }
                        projected.add(apiId.oid());
                    }
                }
            }

            if (map.isEmpty()) {
                return Collections.emptyList();
            }

            Stream<MinimalEntity> entities = Stream.empty();
            try {
                if (!src.isEmpty()) {
                    entities = Stream.concat(entities, repositoryApi.entitiesRequest(src)
                            .getMinimalEntities());
                }
                if (!projected.isEmpty()) {
                    entities = Stream.concat(entities, repositoryApi.entitiesRequest(projected)
                            .projectedTopology()
                            .getMinimalEntities());
                }
            } catch (StatusRuntimeException e) {
                logger.error("Failed to look up entities. Error: {}", e.toString());
                return map.values();
            }

            entities.forEach(minimalEntity -> {
                ApiId id = map.remove(minimalEntity.getOid());
                if (id != null) {
                    id.setCachedEntityInfo(Optional.of(new CachedEntityInfo(minimalEntity)));
                }
            });

            // All the leftovers are not entities.
            return map.values().stream()
                .peek(id -> id.setCachedEntityInfo(Optional.empty()))
                .collect(Collectors.toList());
        }

        /**
         * Resolve a collection of {@link ApiId}s if they refer to groups. Resolve means
         * determine if the id refers to a group and, if so, initialized the cached group-related
         * information for the id.
         *
         * @param apiIds The {@link ApiId}s to resolve.
         * @return A collection of all non-group {@link ApiId}s in the input.
         */
        private Collection<ApiId> bulkResolveGroups(Collection<ApiId> apiIds) {
            final LongObjectMap<ApiId> map = new LongObjectHashMap<>();
            for (ApiId apiId : apiIds) {
                if (apiId.needsResolution()) {
                    map.put(apiId.oid(), apiId);
                }
            }

            if (map.isEmpty()) {
                return Collections.emptyList();
            }

            try {
                final List<GroupAndMembers> groupAndMembers = groupMemberRetriever.getGroupsWithMembers(GetGroupsRequest.newBuilder()
                        .setGroupFilter(GroupFilter.newBuilder().addAllId(map.keySet()))
                        .build());
                if (!groupAndMembers.isEmpty()) {
                    Set<Long> allMembers = groupAndMembers.stream()
                            .flatMap(g -> g.entities().stream())
                            .collect(Collectors.toSet());
                    Map<Long, MinimalEntity> minimalEntitiesById = repositoryApi.entitiesRequest(
                            allMembers).getMinimalEntities()
                        .collect(Collectors.toMap(MinimalEntity::getOid, Function.identity()));
                    groupAndMembers.forEach(group -> {

                        final Map<ApiEntityType, Set<Long>> entityOidsByType = new HashMap<>(1);
                        final Set<Long> discoveringTargetIds = new HashSet<>(1);
                        group.entities()
                                .stream()
                                .map(minimalEntitiesById::get)
                                .filter(Objects::nonNull)
                                .forEach(memberEntity -> {
                                    entityOidsByType.computeIfAbsent(ApiEntityType.fromMinimalEntity(memberEntity),
                                            k -> new HashSet<>()).add(memberEntity.getOid());
                                    discoveringTargetIds.addAll(memberEntity.getDiscoveringTargetIdsList());
                                });

                        final EnvironmentType envTypeFromMember = getEnvironmentType(group.entities()
                                .stream()
                                .map(minimalEntitiesById::get)
                                .filter(Objects::nonNull));

                        CachedGroupInfo groupInfo = new CachedGroupInfo(group.group(),
                                discoveringTargetIds, envTypeFromMember, entityOidsByType);
                        // We remove it from the map, so by the end of the iteration the map contains
                        // only ids that are NOT groups.
                        final ApiId id = map.remove(group.group().getId());
                        id.setCachedGroupInfo(Optional.of(groupInfo));
                    });
                }
            } catch (StatusRuntimeException e) {
                logger.error("Failed to retrieve group information: {}", e.toString());
                // No assumptions about the ids in the map.
                return map.values();
            }

            // All the leftovers are not groups.
            return map.values().stream()
                    .peek(id -> id.setCachedGroupInfo(Optional.empty()))
                    .collect(Collectors.toList());
        }

        /**
         * Resolve a collection of {@link ApiId}s if they refer to plans. Resolve means
         * determine if the id refers to a plan and, if so, initialized the cached plan-related
         * information for the id.
         *
         * @param apiIds The {@link ApiId}s to resolve.
         */
        private void bulkResolvePlans(Collection<ApiId> apiIds) {
            for (ApiId apiId : apiIds) {
                if (!apiId.needsResolution()) {
                    continue;
                }

                final OptionalPlanInstance optPlanInstance;
                try {
                    optPlanInstance = planServiceBlockingStub.getPlan(
                            PlanId.newBuilder().setPlanId(apiId.oid()).build());
                } catch (StatusRuntimeException e) {
                    logger.error("Failed to look up plan for id: {}. Error: {}", apiId.oid(), e.toString());
                    continue;
                }

                if (!optPlanInstance.hasPlanInstance()) {
                    apiId.setCachedPlanInfo(Optional.empty());
                    continue;
                }

                PlanInstance planInstance = optPlanInstance.getPlanInstance();
                ScenarioInfo scenarioInfo = planInstance.getScenario().getScenarioInfo();
                if (!scenarioInfo.hasScope()) {
                    continue;
                }

                PlanScope planScope = scenarioInfo.getScope();
                if (planScope.getScopeEntriesCount() == 0) {
                    continue;
                }

                // Get all the entities in the Plan Scope
                Set<MinimalEntity> minimalEntities = Sets.newHashSet();
                planScope.getScopeEntriesList().stream().forEach(se -> {

                    if (StringConstants.GROUP_TYPES.contains(se.getClassName())) {
                        Optional<GroupAndMembers> groupAndMembers = groupMemberRetriever.getGroupsWithMembers(GetGroupsRequest.newBuilder()
                                .setGroupFilter(GroupFilter.newBuilder()
                                    .addId(se.getScopeObjectOid())
                                    .build())
                                .build()).stream().findFirst();
                        groupAndMembers.ifPresent(andMembers -> repositoryApi.entitiesRequest(
                                Sets.newHashSet(andMembers.entities()))
                                .getMinimalEntities()
                                .forEach(minimalEntities::add));
                    } else {
                        repositoryApi.entityRequest(se.getScopeObjectOid()).getMinimalEntity()
                                .map(minimalEntities::add);
                    }
                });
                // Extract the Environment Type of the entities
                EnvironmentType planEnvType = getEnvironmentType(minimalEntities.stream());

                final Map<ApiEntityType, Set<Long>> entityOidsByType = minimalEntities.stream()
                        .collect(Collectors.groupingBy(
                                ApiEntityType::fromMinimalEntity,
                                Collectors.mapping(MinimalEntity::getOid,
                                        Collectors.toSet())));

                // Add all oids from minimalEntities as scope oid list, that includes members
                // of group, if the plan is scoped to a group (e.g of regions).
                Set<Long> planScopeIds = Sets.newHashSet();
                entityOidsByType.values().forEach(planScopeIds::addAll);

                apiId.setCachedPlanInfo(Optional.of(new CachedPlanInfo(planInstance, planEnvType, planScopeIds,
                        entityOidsByType)));
            }
        }

        private Collection<ApiId> bulkResolveTargets(Collection<ApiId> apiIds) {
            return apiIds.stream()
                    .filter(ApiId::needsResolution)
                    .peek(id -> {
                    final boolean isTarget = thinTargetCache.getTargetInfo(id.oid()).isPresent();
                    id.setIsTarget(isTarget);
                })
                .filter(ApiId::needsResolution)
                .collect(Collectors.toList());
        }

        /**
         * Get the EnvironmentType given a Set of entities.
         *
         * @param minimalEntities Set of entities
         * @return CLOUD if all entities are CLOUD,
         * ON_PREM if all entities are ON_PREM,
         * HYBRID if there are some CLOUD and ON_PREM.
         */
        private EnvironmentType getEnvironmentType(Stream<MinimalEntity> minimalEntities) {
            boolean hasCloud = false;
            boolean hasOnPrem = false;
            Iterator<MinimalEntity> it = minimalEntities.iterator();
            while (it.hasNext()) {
                MinimalEntity me = it.next();
                hasCloud = me.getEnvironmentType() == EnvironmentType.CLOUD || hasCloud;
                hasOnPrem = me.getEnvironmentType() == EnvironmentType.ON_PREM || hasOnPrem;
            }

            EnvironmentType envType = EnvironmentType.UNKNOWN_ENV;
            if (hasCloud && hasOnPrem) {
                envType = EnvironmentType.HYBRID;
            } else if (hasCloud) {
                envType = EnvironmentType.CLOUD;
            } else if (hasOnPrem) {
                envType = EnvironmentType.ON_PREM;
            }

            return envType;
        }

    }

    /**
     * Information about an entity, saved inside an {@link ApiId} referring to an entity.
     */
    public static class CachedEntityInfo {
        private final String displayName;
        private final ApiEntityType entityType;
        private final EnvironmentType environmentType;
        private final Set<Long> discoveringTargetIds;

        public CachedEntityInfo(final MinimalEntity entity) {
            this.displayName = entity.getDisplayName();
            this.entityType = ApiEntityType.fromType(entity.getEntityType());
            this.environmentType = entity.getEnvironmentType();
            this.discoveringTargetIds = Sets.newHashSet(entity.getDiscoveringTargetIdsList());
        }

        @Nonnull
        public ApiEntityType getEntityType() {
            return entityType;
        }

        @Nonnull
        public EnvironmentType getEnvironmentType() {
            return environmentType;
        }

        @Nonnull
        public String getDisplayName() {
            return displayName;
        }

        @Nonnull
        public Set<Long> getDiscoveringTargetIds() {
            return discoveringTargetIds;
        }
    }

    /**
     * Cached information about a group an {@link ApiId} refers to.
     */
    public static class CachedGroupInfo {

        private final boolean globalTempGroup;

        private final EnvironmentType globalTempGroupEnv;

        private final GroupType groupType;

        private final Set<GroupType> nestedGroupTypes;

        private final String name;

        private final Set<Long> discoveringTargetIds;

        private final Map<ApiEntityType, Set<Long>> entityOidsByType;

        /**
         * @param envTypeFromMember the environment type of a member of the group, or
         *                          EnvironmentType.UNKNOWN_ENV if it could not be determined. It
         *                          is used if the group's environment type is not already provided.
         * @param entityOidsByType The entity OIDs contained within the group, indexed by their
         *                         {@link ApiEntityType}.
         */
        private CachedGroupInfo(Grouping group, final Set<Long> discoveringTargetIds,
                EnvironmentType envTypeFromMember, Map<ApiEntityType, Set<Long>> entityOidsByType) {

            this.nestedGroupTypes = group.getExpectedTypesList().stream()
                .filter(GroupDTO.MemberType::hasGroup)
                .map(GroupDTO.MemberType::getGroup)
                .collect(Collectors.toSet());

            // Will be set to false if it's not a temp group, because it's false in the default
            // instance.
            this.globalTempGroup = group.getDefinition().getIsTemporary()
                            && group.getDefinition().hasOptimizationMetadata()
                            && group.getDefinition().getOptimizationMetadata().getIsGlobalScope();
            this.groupType = group.getDefinition().getType();
            this.name = group.getDefinition().getDisplayName();
            this.discoveringTargetIds = discoveringTargetIds;
            this.globalTempGroupEnv = (group.getDefinition().getIsTemporary()
                    && group.getDefinition().hasOptimizationMetadata()
                    && group.getDefinition().getOptimizationMetadata().hasEnvironmentType()) ?
                group.getDefinition().getOptimizationMetadata().getEnvironmentType()
                :  envTypeFromMember;
            this.entityOidsByType = entityOidsByType;
        }

        public boolean isGlobalTempGroup() {
            return globalTempGroup;
        }

        @Nonnull
        public Set<ApiEntityType> getEntityTypes() {
            return Collections.unmodifiableSet(entityOidsByType.keySet());
        }

        /**
         * Returns the type of groups nested in this group.
         * @return the type of nested groups.
         */
        @Nonnull
        public Set<GroupType> getNestedGroupTypes() {
            return Collections.unmodifiableSet(this.nestedGroupTypes);
        }

        @Nonnull
        public Optional<EnvironmentType> getGlobalEnvType() {
            return Optional.of(globalTempGroupEnv).filter(type -> type != EnvironmentType.UNKNOWN_ENV);
        }

        public GroupType getGroupType() {
            return groupType;
        }

        public String getName() {
            return name;
        }

        @Nonnull
        public Set<Long> getDiscoveringTargetIds() {
            return discoveringTargetIds;
        }

        public Set<Long> getEntityIds() {
            return entityOidsByType.values()
                    .stream()
                    .flatMap(Set::stream)
                    .collect(Collectors.toSet());
        }

        public Map<ApiEntityType, Set<Long>> getEntityOidsByType() {
            return Collections.unmodifiableMap(entityOidsByType);
        }
    }

    /**
     * Information about an plan, saved inside an {@link ApiId} referring to an plan.
     */
    public static class CachedPlanInfo {
        private final PlanInstance planInstance;
        private final EnvironmentType environmentType;
        private final Map<ApiEntityType, Set<Long>> entityOidsByType;
        private final Set<Long> planScopeIds;

        public CachedPlanInfo(final PlanInstance planInstance, final EnvironmentType envType,
                              final Set<Long> planScopeIds, final Map<ApiEntityType, Set<Long>> entityOidsByType) {
            this.planInstance = planInstance;
            this.environmentType = envType;
            this.planScopeIds = planScopeIds;
            this.entityOidsByType = entityOidsByType;
        }

        @Nonnull
        public PlanInstance getPlanInstance() {
            return planInstance;
        }

        @Nonnull
        public EnvironmentType getEnvironmentType() {
            return environmentType;
        }

        @Nonnull
        public Set<Long> getPlanScopeIds() {
            return planScopeIds;
        }

        @Nonnull
        public Set<ApiEntityType> getEntityTypes() {
            return Collections.unmodifiableSet(entityOidsByType.keySet());
        }

        @Nonnull
        public Map<ApiEntityType, Set<Long>> getEntityOidsByType() {
            return entityOidsByType;
        }
    }

    /**
     * A class to represent an id for interactions between the external API and XL.
     *
     * The properties of the ID are lazily instantiated - for example, we don't determine whether
     * or not the ID refers to a plan until someone asks about it. This avoids some unnecessary
     * RPC calls, at the expense of memory (for the locks inside the {@link SetOnce} fields) and
     * mutability/complexity. In the future it may be better to initialize all the properties at
     * the time the {@link ApiId} is created.
     */
    @ThreadSafe
    public static class ApiId {
        private final long oid;

        private final long realtimeContextId;

        private Set<Long> targetOids = null;
        private final Object targetOidsLock = new Object();

        /**
         * If this is a group, it will be set to a {@link CachedGroupInfo}.
         * If this is decisively NOT a group, it will be set to an empty {@link Optional}.
         * If we don't know yet, it will be unset.
         * Same for entityInfo and planInfo.
         */
        private final SetOnce<Optional<CachedGroupInfo>> groupInfo = new SetOnce<>();
        private final SetOnce<Optional<CachedEntityInfo>> entityInfo = new SetOnce<>();
        private final SetOnce<Optional<CachedPlanInfo>> planInfo = new SetOnce<>();
        private final SetOnce<Boolean> isTarget = new SetOnce<>();

        private final TopologyProcessor topologyProcessor;

        private final RepositoryApi repositoryApi;

        private final ThinTargetCache thinTargetCache;

        private final CloudTypeMapper cloudTypeMapper;

        private final ApiIdResolver apiIdResolver;

        private static final Supplier<Boolean> FALSE = () -> false;

        private ApiId(final long value,
                      final long realtimeContextId,
                      @Nonnull final ApiIdResolver apiIdResolver,
                      @Nonnull final RepositoryApi repositoryApi,
                      @Nonnull final TopologyProcessor topologyProcessor,
                      @Nonnull final ThinTargetCache thinTargetCache,
                      @Nonnull final CloudTypeMapper cloudTypeMapper) {
            this.oid = value;
            this.realtimeContextId = realtimeContextId;
            this.apiIdResolver = apiIdResolver;
            this.repositoryApi = Objects.requireNonNull(repositoryApi);
            this.topologyProcessor = Objects.requireNonNull(topologyProcessor);
            this.thinTargetCache = thinTargetCache;
            this.cloudTypeMapper = cloudTypeMapper;
            if (isRealtimeMarket()) {
                planInfo.trySetValue(Optional.empty());
                groupInfo.trySetValue(Optional.empty());
                entityInfo.trySetValue(Optional.empty());
                isTarget.trySetValue(false);
            }
        }

        public long oid() {
            return oid;
        }

        /**
         * Returns types in the scope. If the the scope is heterogeneous group it will more than one type.
         * @return types in the scope.
         */
        @Nonnull
        public Optional<Set<ApiEntityType>> getScopeTypes() {
            Optional<Set<ApiEntityType>> scopeTypes = Optional.empty();
            if (isRealtimeMarket()) {
                return Optional.empty();
            } else if (isGroup()) {
                scopeTypes = getCachedGroupInfo()
                        .map(CachedGroupInfo::getEntityTypes);
            } else if (isEntity()) {
                scopeTypes = getCachedEntityInfo()
                        .map(CachedEntityInfo::getEntityType)
                        .map(Collections::singleton);
            } else if (isPlan()) {
                scopeTypes = getCachedPlanInfo()
                        .map(CachedPlanInfo::getEntityTypes);
            }
            return scopeTypes;
        }

        /**
         * Get the entity oids in this scope.
         *
         * @param userSessionContext if not null makes sure user has access to the scope.
         * @param statApiInputDTOList Stats input DTO
         * @return the set of entity oids.
         */
        @Nonnull
        public Set<Long> getScopeOids(@Nullable UserSessionContext userSessionContext,
                                      @Nullable List<StatApiInputDTO> statApiInputDTOList) {
            final Set<Long> result;
            if (isRealtimeMarket()) {
                if (userSessionContext != null && userSessionContext.isUserScoped()) {
                    result = userSessionContext
                        .getUserAccessScope().accessibleOids().toSet();
                } else {
                    // There is not a point of getting all market oids
                    result = Collections.emptySet();
                }
            } else if (isEntity()) {
                result = Collections.singleton(oid);
            } else if (isGroup()) {
                result = getCachedGroupInfo().map(
                        g -> filterEntitiesByCsp(g.getEntityIds(), statApiInputDTOList))
                        .orElse(Collections.emptySet());
            } else if (isPlan()) {
                final Set<Long> entitiesInPlanScope = getCachedPlanInfo().get().getPlanScopeIds();
                result = filterEntitiesByCsp(entitiesInPlanScope, statApiInputDTOList);
            } else if (isTarget()) {
                synchronized (targetOidsLock) {
                    if (targetOids == null) {
                        targetOids = repositoryApi.newSearchRequest(
                            SearchProtoUtil.makeSearchParameters(
                                SearchProtoUtil.discoveredBy(oid()))
                                .build())
                            .getOids();
                    }
                    result = Collections.unmodifiableSet(targetOids);
                }
            } else {
                result = Collections.emptySet();
            }

            if (userSessionContext != null && !isRealtimeMarket() && !isPlan()) {
                UserScopeUtils.checkAccess(userSessionContext, result);
            }
            return result;
        }

        /**
         * Get the entity oids in this scope.
         *
         * @return the set of entity oids.
         */
        @Nonnull
        public Set<Long> getScopeOids() {
            return getScopeOids(null, null);
        }

        @Nonnull
        public String getDisplayName() {
            if (isRealtimeMarket()) {
                return UI_REAL_TIME_MARKET_STR;
            }

            // Right now we are saving the display name of every entity/group in the cached info,
            // only to be used by this method. If this ends up consuming too much memory we can get
            // the display name on demand (the way we do for plan instance).

            final Optional<String> entityName = getCachedEntityInfo()
                .map(CachedEntityInfo::getDisplayName);
            if (entityName.isPresent()) {
                return entityName.get();
            }

            final Optional<String> groupName = getCachedGroupInfo()
                .map(CachedGroupInfo::getName);
            if (groupName.isPresent()) {
                return groupName.get();
            }

            final Optional<String> planName = getCachedPlanInfo()
                    .map(CachedPlanInfo::getPlanInstance)
                    .map(planInstance -> planInstance.getScenario().getScenarioInfo().getName());
            if (planName.isPresent()) {
                return planName.get();
            }

            // TODO (roman, Jul 9 2019): Handle target.
            return uuid();
        }

        /**
         * Get the class name of the api id.
         * This deals with the cases of entities, groups, plans or market.
         * In case of target, and by default, it returns an empty string.
         *
         * @return The class name, or empty string.
         */
        @Nonnull
        public String getClassName() {
            if (isRealtimeMarket() || isPlan()) {
                // If this is the real time market or a plan, return Market as the class
                return UI_REAL_TIME_MARKET_STR;
            }

            // Right now we are saving the display name of every entity/group in the cached info,
            // only to be used by this method. If this ends up consuming too much memory we can get
            // the display name on demand (the way we do for plan instance).

            final Optional<ApiEntityType> entityType = getCachedEntityInfo()
                    .map(CachedEntityInfo::getEntityType);
            if (entityType.isPresent()) {
                return entityType.get().apiStr();
            }

            final Optional<GroupType> groupType = getCachedGroupInfo()
                    .map(CachedGroupInfo::getGroupType);
            if (groupType.isPresent()) {
                // Convert the group type to an api group type
                if (GroupMapper.API_GROUP_TYPE_TO_GROUP_TYPE.inverse()
                        .containsKey(groupType.get())) {
                    return GroupMapper.API_GROUP_TYPE_TO_GROUP_TYPE.inverse().get(groupType.get());
                }
            }

            return "";
        }

        /**
         * Return the environment type related to the entity associated with this api id.
         * Default is UNKNOWN_ENV.
         *
         * @return the environment type or UNKNOWN_ENV.
         */
        @Nonnull
        public EnvironmentType getEnvironmentType() {
            // Assuming Hybrid for a market and plan
            if (isRealtimeMarket() || isPlan()) {
                return EnvironmentType.HYBRID;
            }

            // Right now we are saving the display name of every entity/group in the cached info,
            // only to be used by this method. If this ends up consuming too much memory we can get
            // the display name on demand (the way we do for plan instance).

            final Optional<EnvironmentType> envType = getCachedEntityInfo()
                    .map(CachedEntityInfo::getEnvironmentType);
            if (envType.isPresent()) {
                return envType.get();
            }

            final Optional<CachedGroupInfo> groupInfo = getCachedGroupInfo();
            if (groupInfo.isPresent()) {
                Optional<EnvironmentType> groupEnvType = groupInfo.get().getGlobalEnvType();
                if (groupEnvType.isPresent()) {
                    return groupEnvType.get();
                }
            }

            if (isTarget()) {
                try {
                    final TargetInfo target = topologyProcessor.getTarget(oid);
                    final long probeId = target.getProbeId();
                    final ProbeInfo probeInfo = topologyProcessor.getProbe(probeId);
                    CloudTypeMapper cloudTypeMapper = new CloudTypeMapper();
                    if (cloudTypeMapper.fromTargetType(probeInfo.getType()).isPresent()) {
                        return EnvironmentType.CLOUD;
                    } else {
                        return EnvironmentType.ON_PREM;
                    }
                } catch (TopologyProcessorException | CommunicationException e) {
                    // TopologyProcessorException will never happen, since isTarget() would have
                    // already failed if we can't find the target
                    return EnvironmentType.UNKNOWN_ENV;
                }
            }

            // Default
            return EnvironmentType.UNKNOWN_ENV;
        }

        public String uuid() {
            return isRealtimeMarket() ? UI_REAL_TIME_MARKET_STR : Long.toString(oid);
        }

        public boolean isRealtimeMarket() {
            return oid == realtimeContextId;
        }


        void setCachedEntityInfo(Optional<CachedEntityInfo> cachedEntityInfo) {
            entityInfo.trySetValue(cachedEntityInfo);
            if (cachedEntityInfo.isPresent()) {
                // If it's an entity, it's not a group or plan.
                // We do this outside the entityInfo.ensureSet() to avoid possible deadlocks.
                groupInfo.ensureSet(Optional::empty);
                planInfo.ensureSet(Optional::empty);
                isTarget.ensureSet(FALSE);
            }
        }

        /**
         * If this is an entity, get the cached information about the entity.
         * If this is not an entity, do nothing. This may make an RPC call if the information
         * about this entity is not present yet.
         *
         * @return {@link Optional} containing the {@link CachedEntityInfo}, or an empty optional.
         */
        @Nonnull
        public Optional<CachedEntityInfo> getCachedEntityInfo() {
            if (!entityInfo.getValue().isPresent()) {
                apiIdResolver.bulkResolveEntities(Collections.singleton(this));
            }
            return entityInfo.getValue().orElse(Optional.empty());
        }

        public boolean isEntity() {
            return getCachedEntityInfo().isPresent();
        }

        public boolean isCloudEntity() {
            return getCachedEntityInfo().map(info -> info.getEnvironmentType() == EnvironmentType.CLOUD).orElse(false);
        }

        public boolean isGroup() {
            return getCachedGroupInfo().isPresent();
        }

        /**
         * Check that current scope is resource group or group of resource groups.
         *
         * @return in case of resource group / group of resource groups return true otherwise false
         */
        public boolean isResourceGroupOrGroupOfResourceGroups() {
            boolean isResourceGroupsScope = false;
            if (getGroupType().isPresent()) {
                switch (getGroupType().get()) {
                    case RESOURCE:
                        isResourceGroupsScope = true;
                        break;
                    case REGULAR:
                        if (getCachedGroupInfo().isPresent()) {
                            final Set<GroupType> nestedGroupTypes =
                                    getCachedGroupInfo().get().getNestedGroupTypes();
                            if (!nestedGroupTypes.isEmpty()) {
                                isResourceGroupsScope = nestedGroupTypes.stream()
                                        .allMatch(el -> el.equals(GroupType.RESOURCE));
                            }
                        }
                        break;
                }
            }
            return isResourceGroupsScope;
        }

        public boolean isCloudGroup() {
            return getCachedGroupInfo().flatMap(cgi ->
                    cgi.getGlobalEnvType().map(envType -> envType == EnvironmentType.CLOUD))
                .orElse(false);
        }

        public boolean isCloud() {
            return isCloudEntity() || isCloudGroup() || isCloudPlan();
        }

        public Optional<GroupType> getGroupType() {
            return getCachedGroupInfo().map(CachedGroupInfo::getGroupType);
        }

        void setIsTarget(boolean isTarget) {
            this.isTarget.trySetValue(isTarget);
            if (isTarget) {
                // If it's a target, it's not a group or entity or a plan.
                // We do this outside the isTarget.ensureSet() to avoid possible deadlocks.
                groupInfo.ensureSet(Optional::empty);
                entityInfo.ensureSet(Optional::empty);
                planInfo.ensureSet(Optional::empty);
            }
        }

        /**
         * Return true if this id refers to a target.
         *
         * @return True if this id refers to a target.
         */
        public boolean isTarget() {
            if (!isTarget.getValue().isPresent()) {
                apiIdResolver.bulkResolveTargets(Collections.singleton(this));
            }
            return isTarget.getValue().orElse(false);
        }

        public boolean isGlobalTempGroup() {
            return getCachedGroupInfo().map(CachedGroupInfo::isGlobalTempGroup).orElse(false);
        }

        void setCachedGroupInfo(Optional<CachedGroupInfo> groupInfo) {
            this.groupInfo.trySetValue(groupInfo);
            if (groupInfo.isPresent()) {
                // If it's a group, it's not a plan or entity.
                // Do this outside the groupInfo.ensureSet() to avoid deadlocks.
                entityInfo.ensureSet(Optional::empty);
                planInfo.ensureSet(Optional::empty);
                isTarget.ensureSet(FALSE);
            }
        }

        /**
         * If this entity is a group, get the associated {@link CachedGroupInfo}.
         * May result in an API call.
         *
         * @return The {@link CachedGroupInfo} or optional.
         */
        @Nonnull
        public Optional<CachedGroupInfo> getCachedGroupInfo() {
            if (!groupInfo.getValue().isPresent()) {
                apiIdResolver.bulkResolveGroups(Collections.singleton(this));
            }
            return groupInfo.getValue().orElse(Optional.empty());
        }


        void setCachedPlanInfo(Optional<CachedPlanInfo> cachedPlanInfo) {
            planInfo.trySetValue(cachedPlanInfo);
            if (cachedPlanInfo.isPresent()) {
                // If it's a plan, it's not a group or entity.
                // Do this outside the planInfo.ensureSet() to avoid deadlocks.
                entityInfo.ensureSet(Optional::empty);
                groupInfo.ensureSet(Optional::empty);
                isTarget.ensureSet(FALSE);
            }
        }

        /**
         * If this entity is a plan, get the associated {@link CachedPlanInfo}.
         * May result in an API call.
         *
         * @return The {@link CachedPlanInfo} or optional.
         */
        @Nonnull
        public Optional<CachedPlanInfo> getCachedPlanInfo() {
            if (!planInfo.getValue().isPresent()) {
                apiIdResolver.bulkResolvePlans(Collections.singleton(this));
            }
            return planInfo.getValue().orElse(Optional.empty());
        }

        public boolean isCloudPlan() {
            return getCachedPlanInfo()
                    .map(info -> info.getEnvironmentType() == EnvironmentType.CLOUD)
                    .orElse(false);
        }

        public boolean isPlan() {
            return getCachedPlanInfo().isPresent();
        }

        boolean needsResolution() {
            // This ID needs resolution if:
            // 1) It's not the realtime market, and
            // 2) It's not POSITIVELY one of "group", "entity", "plan", or "target."
            //    In particular, if we know it's NOT a "group" (i.e. groupInfo.getValue().isPresent())
            //    it still needs resolution unless we know what it is (e.g. planInfo.getValue().get().isPresent()).
            return !(isRealtimeMarket() || groupInfo.getValue().map(Optional::isPresent).orElse(false)
                || entityInfo.getValue().map(Optional::isPresent).orElse(false)
                || planInfo.getValue().map(Optional::isPresent).orElse(false)
                || isTarget.getValue().orElse(false));
        }

        /**
         * Get all the discovering target ids.
         *
         * @return A {@link Set} of target Ids.
         */
        public Set<Long> getDiscoveringTargetIds() {
            if (isGroup()) {
                return getCachedGroupInfo().get().getDiscoveringTargetIds();
            } else if (isTarget()) {
                return Collections.singleton(oid);
            } else if (isPlan()) {
                return Collections.emptySet();
            } else {
                return getCachedEntityInfo().map(CachedEntityInfo::getDiscoveringTargetIds)
                    .orElseGet(Collections::emptySet);
            }
        }

        @Nonnull
        public Map<ApiEntityType, Set<Long>> getScopeEntitiesByType() {
            return getCachedGroupInfo()
                    .map(CachedGroupInfo::getEntityOidsByType)
                    .orElseGet(() ->
                            getCachedEntityInfo()
                                    .map(entityInfo ->
                                            Collections.singletonMap(
                                                    entityInfo.getEntityType(),
                                                    Collections.singleton(oid)))
                                    .orElseGet(() -> getCachedPlanInfo().map(CachedPlanInfo::getEntityOidsByType)
                                            .orElse(Collections.emptyMap())));
        }

        /**
         * Determines the topology context ID of this scope.
         *
         * @return If this scope is a plan scope, returns the plan ID. If the scope is not a plan
         * instance (based on {@link #getCachedPlanInfo()}), returns the realtime context ID.
         */
        public long getTopologyContextId() {
            return getCachedPlanInfo()
                    .map(CachedPlanInfo::getPlanInstance)
                    .map(PlanInstance::getPlanId)
                    .orElse(realtimeContextId);
        }

        @Override
        public boolean equals(Object other) {
            if (other == this) return true;
            if (other instanceof ApiId) {
                return this.oid == ((ApiId)other).oid;
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Long.hashCode(this.oid);
        }

        /**
         * Filter a list of scope OIDs by CSP.
         *
         * @param scopeOids a set of OIDs.
         * @param statApiInputDTOList DTO that can have filters for CSP.
         * @return The subset of scope OIDs that belong to the CSPs indicated in the input filter.
         */
        @VisibleForTesting
        Set<Long> filterEntitiesByCsp(Set<Long> scopeOids, List<StatApiInputDTO> statApiInputDTOList) {
            Set<String> cspFilter = getCspFilter(statApiInputDTOList);
            if (!cspFilter.isEmpty()) {
                scopeOids = repositoryApi.entitiesRequest(scopeOids).getMinimalEntities()
                        .filter(e -> {
                            if (!e.getDiscoveringTargetIdsList().isEmpty()) {
                                for (long targetId : e.getDiscoveringTargetIdsList()) {
                                    Optional<ThinTargetCache.ThinTargetInfo> thinInfo =
                                            thinTargetCache.getTargetInfo(targetId);
                                    if (thinInfo.isPresent() && (!thinInfo.get().isHidden())) {
                                        ThinTargetCache.ThinTargetInfo probeInfo = thinInfo.get();
                                        Optional<CloudType> cloudType =
                                                cloudTypeMapper.fromTargetType(probeInfo.probeInfo().type());
                                        if (cloudType.isPresent() && cspFilter.contains(cloudType.get().name())) {
                                            return true;
                                        }
                                    }
                                }
                            }
                            return false;
                        })
                        .map(MinimalEntity::getOid)
                        .collect(Collectors.toSet());
            }
            return scopeOids;
        }

        /**
         * The statApiInput may include a filter criterion for CSPs. If the filter is present,
         * return entities that belong to the indicated CSPs.
         *
         * @param statApiInput API input for the stats request.
         * @return a set of entity OIDs filtered by CSP.
         */
        private Set<String> getCspFilter(List<StatApiInputDTO> statApiInput) {
            if (statApiInput != null && statApiInput.size() > 0) {
                return statApiInput.stream().flatMap(input -> {
                    if (input.getFilters() != null) {
                        return input.getFilters().stream()
                                .filter(f -> f.getType().equalsIgnoreCase(StringConstants.CSP))
                                .map(StatFilterApiDTO::getValue);
                    }
                    return Stream.empty();
                }).collect(Collectors.toSet());
            }
            return Collections.emptySet();
        }
    }

    private static class Metrics {

        static final DataMetricCounter CACHE_HIT_COUNT = DataMetricCounter.builder()
            .withName("api_uuid_mapper_cache_hit_count")
            .withHelp("Number of UUID mappings that hit the cache.")
            .build()
            .register();

        static final DataMetricCounter CACHE_MISS_COUNT = DataMetricCounter.builder()
            .withName("api_uuid_mapper_cache_miss_count")
            .withHelp("Number of UUID mappings that miss the cache.")
            .build()
            .register();
    }
}
