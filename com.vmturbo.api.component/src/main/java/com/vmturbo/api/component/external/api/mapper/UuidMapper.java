package com.vmturbo.api.component.external.api.mapper;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.Sets;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.util.DefaultCloudGroup;
import com.vmturbo.api.component.external.api.util.DefaultCloudGroupProducer;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.MagicScopeGateway;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.UserScopeUtils;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.OptionalPlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.repository.api.RepositoryListener;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorException;

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

    private final PlanServiceBlockingStub planServiceBlockingStub;

    private final GroupServiceBlockingStub groupServiceBlockingStub;

    private final GroupExpander groupExpander;

    private final RepositoryApi repositoryApi;

    private final TopologyProcessor topologyProcessor;

    private final MagicScopeGateway magicScopeGateway;

    /**
     * We cache the {@link ApiId}s associated with specific OIDs, so that we can save the
     * information about each ID and avoid extra RPCs to determine whether type the ID refers to.
     *
     * <p>We don't expect this map to be huge, because most entities (probably) aren't going to be
     * addressed by ID.</p>
     * <p>This cache cleared when new topology is available in repository.</p>
     */
    private final Map<Long, ApiId> cachedIds = Collections.synchronizedMap(new HashMap<>());

    public UuidMapper(final long realtimeContextId,
                      @Nonnull final MagicScopeGateway magicScopeGateway,
                      @Nonnull final RepositoryApi repositoryApi,
                      @Nonnull final TopologyProcessor topologyProcessor,
                      @Nonnull final PlanServiceBlockingStub planServiceBlockingStub,
                      @Nonnull final GroupServiceBlockingStub groupServiceBlockingStub,
                      @Nonnull final GroupExpander groupExpander) {
        this.realtimeContextId = realtimeContextId;
        this.magicScopeGateway = magicScopeGateway;
        this.repositoryApi = repositoryApi;
        this.topologyProcessor = topologyProcessor;
        this.planServiceBlockingStub = planServiceBlockingStub;
        this.groupServiceBlockingStub = groupServiceBlockingStub;
        this.groupExpander = groupExpander;
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
                return new ApiId(oid, realtimeContextId, repositoryApi, topologyProcessor,
                    planServiceBlockingStub, groupServiceBlockingStub, groupExpander);
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
                return new ApiId(oid, realtimeContextId, repositoryApi, topologyProcessor,
                    planServiceBlockingStub, groupServiceBlockingStub, groupExpander);
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
            logger.info("Clear all cached {@link ApiId}'s associated with specific OID when new " +
                    "topology (topologyId - {}) received ", topologyId);
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
                : envTypeFromMember;
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

        private final SetOnce<Boolean> isPlan = new SetOnce<>();

        private Set<Long> targetOids = null;
        private final Object targetOidsLock = new Object();

        /**
         * If this is a group, it will be set to a {@link CachedGroupInfo}.
         * If this is decisively NOT a group, it will be set to an empty {@link Optional}.
         * If we don't know yet, it will be unset.
         */
        private final SetOnce<Optional<CachedGroupInfo>> groupInfo = new SetOnce<>();

        private final SetOnce<Optional<CachedEntityInfo>> entityInfo = new SetOnce<>();

        private final SetOnce<Boolean> isTarget = new SetOnce<>();

        private final PlanServiceBlockingStub planServiceBlockingStub;

        private final GroupServiceBlockingStub groupServiceBlockingStub;

        private final GroupExpander groupExpander;

        private final TopologyProcessor topologyProcessor;

        private final RepositoryApi repositoryApi;

        private static final Supplier<Boolean> FALSE = () -> false;

        private ApiId(final long value,
                      final long realtimeContextId,
                      @Nonnull final RepositoryApi repositoryApi,
                      @Nonnull final TopologyProcessor topologyProcessor,
                      @Nonnull final PlanServiceBlockingStub planServiceBlockingStub,
                      @Nonnull final GroupServiceBlockingStub groupServiceBlockingStub,
                      @Nonnull final GroupExpander groupExpander) {
            this.oid = value;
            this.realtimeContextId = realtimeContextId;
            this.repositoryApi = Objects.requireNonNull(repositoryApi);
            this.topologyProcessor = Objects.requireNonNull(topologyProcessor);
            this.planServiceBlockingStub = Objects.requireNonNull(planServiceBlockingStub);
            this.groupServiceBlockingStub = Objects.requireNonNull(groupServiceBlockingStub);
            this.groupExpander = groupExpander;
            if (isRealtimeMarket()) {
                isPlan.trySetValue(false);
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
                scopeTypes = getPlanInstance().map(MarketMapper::getPlanScopeTypes);
            }
            return scopeTypes;
        }

        /**
         * Get the entity oids in this scope.
         *
         * @param userSessionContext if not null makes sure user has access to the scope.
         * @return the set of entity oids.
         */
        @Nonnull
        public Set<Long> getScopeOids(@Nullable UserSessionContext userSessionContext) {
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
                result = getCachedGroupInfo().get().getEntityIds();
            } else if (isPlan()) {
                result = getPlanInstance()
                    .map(MarketMapper::getPlanScopeIds)
                    .orElse(Collections.emptySet());
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
            return getScopeOids(null);
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

            final Optional<String> planName = getPlanInstance()
                .map(planInstance -> planInstance.getScenario().getScenarioInfo().getName());
            if (planName.isPresent()) {
                return planName.get();
            }

            // TODO (roman, Jul 9 2019): Handle target.
            return uuid();
        }

        public String uuid() {
            return isRealtimeMarket() ? UI_REAL_TIME_MARKET_STR : Long.toString(oid);
        }

        public boolean isRealtimeMarket() {
            return oid == realtimeContextId;
        }

        private Optional<CachedEntityInfo> getCachedEntityInfo() {
            final Optional<CachedEntityInfo> newCachedInfo = entityInfo.ensureSet(() -> {
                try {
                    // For all non-negative IDs we only look in the "source" topology.
                    // For entities the market provisions (e.g. when it recommends adding a host)
                    // we expect IDs to be negative, and look for them in the projected topology.
                    //
                    // Note - this won't work for entities in a plan, because we don't save the plan
                    // source topology, but the ApiId isn't supposed to be scoped inside a plan anyway.
                    if (oid >= 0) {
                        return repositoryApi.entityRequest(oid)
                            .getMinimalEntity()
                            .map(CachedEntityInfo::new);
                    } else {
                        return repositoryApi.entityRequest(oid)
                            .projectedTopology()
                            .getMinimalEntity()
                            .map(CachedEntityInfo::new);
                    }
                } catch (StatusRuntimeException e) {
                    // Return null to leave the SetOnce value unset - we still don't know!
                    return null;
                }
            });

            final boolean newIsEntity = newCachedInfo != null && newCachedInfo.isPresent();
            if (newIsEntity) {
                // If it's an entity, it's not a group or plan.
                // We do this outside the entityInfo.ensureSet() to avoid possible deadlocks.
                groupInfo.ensureSet(Optional::empty);
                isPlan.ensureSet(FALSE);
                isTarget.ensureSet(FALSE);
            }
            return newCachedInfo == null ? Optional.empty() : newCachedInfo;
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
            return isCloudEntity() || isCloudGroup();
        }

        public Optional<GroupType> getGroupType() {
            return getCachedGroupInfo().map(CachedGroupInfo::getGroupType);
        }

        public boolean isTarget() {
            final Boolean newIsTarget = isTarget.ensureSet(() -> {
                try {
                    final TargetInfo targetInfo = topologyProcessor.getTarget(oid);
                    // If we get a target info, it's a target.
                    return true;
                } catch (TopologyProcessorException e) {
                    return false;
                } catch (CommunicationException e) {
                    // Return null to leave the SetOnce value unset - we still don't know!
                    return null;
                }
            });

            final boolean retIsTarget = newIsTarget != null && newIsTarget;
            if (retIsTarget) {
                // If it's a plan, it's not a group or entity.
                // We do this outside the isPlan.ensureSet() to avoid possible deadlocks.
                groupInfo.ensureSet(Optional::empty);
                entityInfo.ensureSet(Optional::empty);
                isPlan.ensureSet(FALSE);
            }
            return retIsTarget;
        }

        public boolean isGlobalTempGroup() {
            return getCachedGroupInfo().map(CachedGroupInfo::isGlobalTempGroup).orElse(false);
        }

        @Nonnull
        public Optional<CachedGroupInfo> getCachedGroupInfo() {
            Optional<CachedGroupInfo> cachedInfoOpt = groupInfo.ensureSet(() -> {
                try {
                    final GetGroupResponse resp = groupServiceBlockingStub.getGroup(GroupID.newBuilder()
                        .setId(oid)
                        .build());
                    if (resp.hasGroup()) {
                        final Collection<Long> entityOids =
                            groupExpander.getMembersForGroup(resp.getGroup()).entities();
                        final Set<MinimalEntity> minimalMembers =
                            repositoryApi.entitiesRequest(Sets.newHashSet(entityOids)).getMinimalEntities()
                                .collect(Collectors.toSet());
                        final Map<ApiEntityType, Set<Long>> entityOidsByType = minimalMembers.stream()
                                .collect(Collectors.groupingBy(
                                        ApiEntityType::fromMinimalEntity,
                                        Collectors.mapping(MinimalEntity::getOid,
                                                Collectors.toSet())));

                        final EnvironmentType envTypeFromMember = minimalMembers.isEmpty() ?
                            EnvironmentType.UNKNOWN_ENV :
                            minimalMembers.iterator().next().getEnvironmentType();

                        final Set<Long> discoveringTargetIds =
                            minimalMembers.stream()
                                .flatMap(minEntity -> minEntity.getDiscoveringTargetIdsList().stream())
                                .collect(Collectors.toSet());
                        return Optional.of(new CachedGroupInfo(resp.getGroup(), discoveringTargetIds,
                            envTypeFromMember, entityOidsByType));
                    } else {
                        return Optional.empty();
                    }
                } catch (StatusRuntimeException e) {
                    // Return null to leave the SetOnce value unset - we still don't know!
                    return null;
                }
            });

            if (cachedInfoOpt != null && cachedInfoOpt.isPresent()) {
                // If it's a group, it's not a plan or entity.
                // Do this outside the groupInfo.ensureSet() to avoid deadlocks.
                isPlan.ensureSet(FALSE);
                entityInfo.ensureSet(Optional::empty);
                isTarget.ensureSet(FALSE);
            }

            return cachedInfoOpt != null ? cachedInfoOpt : Optional.empty();
        }

        private SetOnce<PlanInstance> checkPlanInstance() {
            SetOnce<PlanInstance> instance = new SetOnce<>();
            final Boolean newIsPlan = isPlan.ensureSet(() -> {
                try {
                    OptionalPlanInstance optPlanInstance = planServiceBlockingStub.getPlan(PlanId.newBuilder()
                        .setPlanId(oid)
                        .build());
                    if (optPlanInstance.hasPlanInstance()) {
                        instance.trySetValue(optPlanInstance.getPlanInstance());
                        return true;
                    } else {
                        return false;
                    }
                } catch (StatusRuntimeException e) {
                    // Return null to leave the SetOnce value unset - we still don't know!
                    return null;
                }
            });

            if (newIsPlan != null && newIsPlan) {
                // If it's a plan, it's not a group or entity.
                // We do this outside the isPlan.ensureSet() to avoid possible deadlocks.
                groupInfo.ensureSet(Optional::empty);
                entityInfo.ensureSet(Optional::empty);
                isTarget.ensureSet(FALSE);
            }

            return instance;
        }

        @Nonnull
        public Optional<PlanInstance> getPlanInstance() {
            final SetOnce<PlanInstance> instance = checkPlanInstance();
            if (isPlan.getValue().orElse(false)) {
                // It may already be set, or it may not be.
                return Optional.ofNullable(instance.ensureSet(() -> {
                    try {
                        OptionalPlanInstance optPlanInstance = planServiceBlockingStub.getPlan(PlanId.newBuilder()
                            .setPlanId(oid)
                            .build());
                        if (optPlanInstance.hasPlanInstance()) {
                            return optPlanInstance.getPlanInstance();
                        } else {
                            // It's possible the plan got deleted out from underneath.
                            return null;
                        }
                    } catch (StatusRuntimeException e) {
                        return null;
                    }
                }));
            } else {
                return Optional.empty();
            }
        }

        public boolean isPlan() {
            checkPlanInstance();
            return isPlan.getValue().orElse(false);
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
                                    .orElse(Collections.emptyMap()));
        }

        /**
         * Determines the topology context ID of this scope.
         *
         * @return If this scope is a plan scope, returns the plan ID. If the scope is not a plan
         * instance (based on {@link #getPlanInstance()}), returns the realtime context ID.
         */
        public long getTopologyContextId() {
            return getPlanInstance()
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
