package com.vmturbo.api.component.external.api.mapper;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.springframework.util.CollectionUtils;

import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.util.DefaultCloudGroup;
import com.vmturbo.api.component.external.api.util.DefaultCloudGroupProducer;
import com.vmturbo.api.component.external.api.util.MagicScopeGateway;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.OptionalPlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorException;

/**
 * Mapper to convert string UUID's to OID's that make sense in the
 * XL system. This class, in addition to {@link ApiId}, should encapsulate
 * all the weird constants, corner-cases, and magic strings involved in dealing
 * with the UI requests.
 */
public class UuidMapper {

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

    private final RepositoryApi repositoryApi;

    private final TopologyProcessor topologyProcessor;

    private final MagicScopeGateway magicScopeGateway;

    /**
     * We cache the {@link ApiId}s associated with specific OIDs, so that we can save the
     * information about each ID and avoid extra RPCs to determine whether type the ID refers to.
     * <p>
     * We don't expect this map to be huge, because most entities (probably) aren't going to be
     * addressed by ID.
     */
    private final Map<Long, ApiId> cachedIds = Collections.synchronizedMap(new HashMap<>());

    public UuidMapper(final long realtimeContextId,
                      @Nonnull final MagicScopeGateway magicScopeGateway,
                      @Nonnull final RepositoryApi repositoryApi,
                      @Nonnull final TopologyProcessor topologyProcessor,
                      @Nonnull final PlanServiceBlockingStub planServiceBlockingStub,
                      @Nonnull final GroupServiceBlockingStub groupServiceBlockingStub) {
        this.realtimeContextId = realtimeContextId;
        this.magicScopeGateway = magicScopeGateway;
        this.repositoryApi = repositoryApi;
        this.topologyProcessor = topologyProcessor;
        this.planServiceBlockingStub = planServiceBlockingStub;
        this.groupServiceBlockingStub = groupServiceBlockingStub;
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
                return new ApiId(oid, realtimeContextId, repositoryApi,
                    topologyProcessor, planServiceBlockingStub, groupServiceBlockingStub);
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
                return new ApiId(oid, realtimeContextId, repositoryApi,
                    topologyProcessor, planServiceBlockingStub, groupServiceBlockingStub);
            } else {
                Metrics.CACHE_HIT_COUNT.increment();
                return existing;
            }
        });
    }

    public static boolean isRealtimeMarket(String uuid) {
        return uuid.equals(UI_REAL_TIME_MARKET_STR);
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
        private final UIEntityType entityType;
        private final EnvironmentType environmentType;

        public CachedEntityInfo(final MinimalEntity entity) {
            this.displayName = entity.getDisplayName();
            this.entityType = UIEntityType.fromType(entity.getEntityType());
            this.environmentType = entity.getEnvironmentType();
        }

        @Nonnull
        public UIEntityType getEntityType() {
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
    }

    /**
     * Cached information about a group an {@link ApiId} refers to.
     */
    public static class CachedGroupInfo {

        private final boolean globalTempGroup;

        private final Type groupType;

        private final UIEntityType entityType;

        private final String name;

        private CachedGroupInfo(Group group) {
            this.entityType = UIEntityType.fromType(GroupProtoUtil.getEntityType(group));
            // Will be set to false if it's not a temp group, because it's false in the default
            // instance.
            this.globalTempGroup = group.getTempGroup().getIsGlobalScopeGroup();
            this.groupType = group.getType();
            this.name = GroupProtoUtil.getGroupName(group);
        }

        public boolean isGlobalTempGroup() {
            return globalTempGroup;
        }

        public UIEntityType getEntityType() {
            return entityType;
        }

        public Type getGroupType() {
            return groupType;
        }

        public String getName() {
            return name;
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

        private final TopologyProcessor topologyProcessor;

        private final RepositoryApi repositoryApi;

        private static final Supplier<Boolean> FALSE = () -> false;

        private ApiId(final long value,
                      final long realtimeContextId,
                      @Nonnull final RepositoryApi repositoryApi,
                      @Nonnull final TopologyProcessor topologyProcessor,
                      @Nonnull final PlanServiceBlockingStub planServiceBlockingStub,
                      @Nonnull final GroupServiceBlockingStub groupServiceBlockingStub) {
            this.oid = value;
            this.realtimeContextId = realtimeContextId;
            this.repositoryApi = Objects.requireNonNull(repositoryApi);
            this.topologyProcessor = Objects.requireNonNull(topologyProcessor);
            this.planServiceBlockingStub = Objects.requireNonNull(planServiceBlockingStub);
            this.groupServiceBlockingStub = Objects.requireNonNull(groupServiceBlockingStub);
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

        @Nonnull
        public Optional<UIEntityType> getScopeType() {
            if (isRealtimeMarket()) {
                return Optional.empty();
            }

            final Optional<UIEntityType> groupType = getCachedGroupInfo().map(CachedGroupInfo::getEntityType);
            if (groupType.isPresent()) {
                return groupType;
            }

            return getCachedEntityInfo().map(CachedEntityInfo::getEntityType);
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

        public Optional<Group.Type> getGroupType() {
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
                        return Optional.of(new CachedGroupInfo(resp.getGroup()));
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
