package com.vmturbo.api.component.external.api.mapper;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.springframework.util.CollectionUtils;

import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.SetOnce;

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

    private final long realtimeContextId;

    private final PlanServiceBlockingStub planServiceBlockingStub;

    private final GroupServiceBlockingStub groupServiceBlockingStub;

    private final RepositoryApi repositoryApi;

    /**
     * We cache the {@link ApiId}s associated with specific OIDs, so that we can save the
     * information about each ID and avoid extra RPCs to determine whether type the ID refers to.
     * <p>
     * We don't expect this map to be huge, because most entities (probably) aren't going to be
     * addressed by ID.
     */
    private final Map<Long, ApiId> cachedIds = Collections.synchronizedMap(new HashMap<>());

    public UuidMapper(final long realtimeContextId,
                      @Nonnull final RepositoryApi repositoryApi,
                      @Nonnull final PlanServiceBlockingStub planServiceBlockingStub,
                      @Nonnull final GroupServiceBlockingStub groupServiceBlockingStub) {
        this.realtimeContextId = realtimeContextId;
        this.repositoryApi = repositoryApi;
        this.planServiceBlockingStub = planServiceBlockingStub;
        this.groupServiceBlockingStub = groupServiceBlockingStub;
    }

    @Nonnull
    public ApiId fromUuid(@Nonnull final String uuid) {
        final boolean isRealtime = uuid.equals(UI_REAL_TIME_MARKET_STR);
        final long oid = isRealtime ? realtimeContextId : Long.valueOf(uuid);
        return cachedIds.computeIfAbsent(oid, k -> new ApiId(oid, realtimeContextId, repositoryApi,
            planServiceBlockingStub, groupServiceBlockingStub));
    }

    @Nonnull
    public ApiId fromOid(final long oid) {
        return cachedIds.computeIfAbsent(oid, k -> new ApiId(oid, realtimeContextId, repositoryApi,
            planServiceBlockingStub, groupServiceBlockingStub));
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
     * Cached information about a group an {@link ApiId} refers to.
     */
    public static class CachedGroupInfo {

        private final boolean globalTempGroup;

        private final UIEntityType entityType;

        private CachedGroupInfo(Group group) {
            this.entityType = UIEntityType.fromType(GroupProtoUtil.getEntityType(group));
            // Will be set to false if it's not a temp group, because it's false in the default
            // instance.
            this.globalTempGroup = group.getTempGroup().getIsGlobalScopeGroup();
        }

        public boolean isGlobalTempGroup() {
            return globalTempGroup;
        }

        public UIEntityType getEntityType() {
            return entityType;
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

        private final SetOnce<Boolean> isEntity = new SetOnce<>();

        private final PlanServiceBlockingStub planServiceBlockingStub;

        private final GroupServiceBlockingStub groupServiceBlockingStub;

        private final RepositoryApi repositoryApi;

        private static final Supplier<Boolean> FALSE = () -> false;

        private ApiId(final long value,
                      final long realtimeContextId,
                      @Nonnull final RepositoryApi repositoryApi,
                      @Nonnull final PlanServiceBlockingStub planServiceBlockingStub,
                      @Nonnull final GroupServiceBlockingStub groupServiceBlockingStub) {
            this.oid = value;
            this.realtimeContextId = realtimeContextId;
            this.repositoryApi = Objects.requireNonNull(repositoryApi);
            this.planServiceBlockingStub = Objects.requireNonNull(planServiceBlockingStub);
            this.groupServiceBlockingStub = Objects.requireNonNull(groupServiceBlockingStub);
            if (isRealtimeMarket()) {
                isPlan.trySetValue(false);
                groupInfo.trySetValue(Optional.empty());
                isEntity.trySetValue(false);
            }
        }

        public long oid() {
            return oid;
        }

        public String uuid() {
            return isRealtimeMarket() ? UI_REAL_TIME_MARKET_STR : Long.toString(oid);
        }

        public boolean isRealtimeMarket() {
            return oid == realtimeContextId;
        }

        public boolean isEntity() {
            final Boolean isEntityVal = isEntity.ensureSet(() -> {
                try {
                    // For all non-negative IDs we only look in the "source" topology.
                    // For entities the market provisions (e.g. when it recommends adding a host)
                    // we expect IDs to be negative, and look for them in the projected topology.
                    //
                    // Note - this won't work for entities in a plan, because we don't save the plan
                    // source topology, but the ApiId isn't supposed to be scoped inside a plan anyway.
                    if (oid >= 0) {
                        return repositoryApi.entityRequest(oid)
                            .getMinimalEntity().isPresent();
                    } else {
                        return repositoryApi.entityRequest(oid)
                            .projectedTopology()
                            .getMinimalEntity().isPresent();
                    }
                } catch (StatusRuntimeException e) {
                    // Return null to leave the SetOnce value unset - we still don't know!
                    return null;
                }
            });

            final boolean newIsEntity = isEntityVal != null && isEntityVal;
            if (newIsEntity) {
                // If it's an entity, it's not a group or plan.
                // We do this outside the isEntity.ensureSet() to avoid possible deadlocks.
                groupInfo.ensureSet(Optional::empty);
                isPlan.ensureSet(FALSE);
            }
            return newIsEntity;
        }

        public boolean isGroup() {
            return getCachedGroupInfo().isPresent();
        }

        public boolean isGlobalTempGroup() {
            return getCachedGroupInfo().map(CachedGroupInfo::isGlobalTempGroup).orElse(false);
        }

        @Nonnull
        public Optional<UIEntityType> getGroupEntityType() {
            return getCachedGroupInfo().map(CachedGroupInfo::getEntityType);
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
                isEntity.ensureSet(FALSE);
            }

            return cachedInfoOpt != null ? cachedInfoOpt : Optional.empty();
        }

        public boolean isPlan() {
            final Boolean isPlanVal = isPlan.ensureSet(() -> {
                try {
                    return planServiceBlockingStub.getPlan(PlanId.newBuilder()
                        .setPlanId(oid)
                        .build()).hasPlanInstance();
                } catch (StatusRuntimeException e) {
                    // Return null to leave the SetOnce value unset - we still don't know!
                    return null;
                }
            });
            final boolean newIsPlan = isPlanVal != null && isPlanVal;
            if (newIsPlan) {
                // If it's a plan, it's not a group or entity.
                // We do this outside the isPlan.ensureSet() to avoid possible deadlocks.
                groupInfo.ensureSet(Optional::empty);
                isEntity.ensureSet(FALSE);
            }
            return newIsPlan;
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
}
