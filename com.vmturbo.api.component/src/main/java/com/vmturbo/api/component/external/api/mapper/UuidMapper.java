package com.vmturbo.api.component.external.api.mapper;

import java.util.Collection;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.springframework.util.CollectionUtils;

import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
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

    public UuidMapper(final long realtimeContextId,
                      @Nonnull final PlanServiceBlockingStub planServiceBlockingStub,
                      @Nonnull final GroupServiceBlockingStub groupServiceBlockingStub) {
        this.realtimeContextId = realtimeContextId;
        this.planServiceBlockingStub = planServiceBlockingStub;
        this.groupServiceBlockingStub = groupServiceBlockingStub;
    }

    @Nonnull
    public ApiId fromUuid(String uuid) {
        final boolean isRealtime = uuid.equals(UI_REAL_TIME_MARKET_STR);
        return new ApiId(isRealtime ? realtimeContextId : Long.valueOf(uuid), uuid,
            planServiceBlockingStub,
            groupServiceBlockingStub);
    }

    @Nonnull
    public ApiId fromOid(long oid) {
        final boolean isRealtime = oid == realtimeContextId;
        return new ApiId(oid, isRealtime ? UI_REAL_TIME_MARKET_STR : Long.toString(oid),
            planServiceBlockingStub, groupServiceBlockingStub);
    }

    /**
     * A class to represent an id for interactions between the external API and XL.
     *
     * TODO (roman, Feb 26 2019): OM-43459 - The UuidMapper should cache the isGroup, isPlan, and
     * isEntity results and feed them to each ApiId.
     */
    public static class ApiId {
        private final long oid;
        private final String uuid;
        private final SetOnce<Boolean> isPlan = new SetOnce<>();
        private final SetOnce<Boolean> isGroup = new SetOnce<>();
        private final SetOnce<Boolean> isEntity = new SetOnce<>();
        private final PlanServiceBlockingStub planServiceBlockingStub;
        private final GroupServiceBlockingStub groupServiceBlockingStub;

        private ApiId(final long value,
                      @Nonnull final String uuid,
                      @Nonnull final PlanServiceBlockingStub planServiceBlockingStub,
                      @Nonnull final GroupServiceBlockingStub groupServiceBlockingStub) {
            this.oid = value;
            this.uuid = Objects.requireNonNull(uuid);
            this.planServiceBlockingStub = Objects.requireNonNull(planServiceBlockingStub);
            this.groupServiceBlockingStub = Objects.requireNonNull(groupServiceBlockingStub);
            if (isRealtimeMarket()) {
                isPlan.trySetValue(false);
                isGroup.trySetValue(false);
                isEntity.trySetValue(false);
            }
        }

        public long oid() {
            return oid;
        }

        public String uuid() {
            return uuid;
        }

        public boolean isRealtimeMarket() {
            return uuid.equals(UI_REAL_TIME_MARKET_STR);
        }

        public synchronized boolean isEntity() {
            if (isEntity.getValue().isPresent()) {
                return isEntity.getValue().get();
            } else {
                final boolean newIsEntity = !(isRealtimeMarket() || isGroup() || isPlan());
                isEntity.trySetValue(newIsEntity);
                return newIsEntity;
            }
        }

        public synchronized boolean isGroup() {
            if (isGroup.getValue().isPresent()) {
                return isGroup.getValue().get();
            } else {
                try {
                    final boolean newIsGroup = groupServiceBlockingStub.getGroup(GroupID.newBuilder()
                        .setId(oid)
                        .build()).hasGroup();
                    isGroup.trySetValue(newIsGroup);
                    if (newIsGroup) {
                        // If it's a group, we know it's not a plan or entity.
                        isPlan.trySetValue(false);
                        isEntity.trySetValue(false);
                    }
                    return newIsGroup;
                } catch (StatusRuntimeException e) {
                    // Don't set the "isGroup" variable - we still don't know!
                    return false;
                }
            }
        }

        public synchronized boolean isPlan() {
            if (isPlan.getValue().isPresent()) {
                return isPlan.getValue().get();
            } else {
                try {
                    final boolean newIsPlan = planServiceBlockingStub.getPlan(PlanId.newBuilder()
                        .setPlanId(oid)
                        .build()).hasPlanInstance();
                    isPlan.trySetValue(newIsPlan);
                    if (newIsPlan) {
                        // If it's a plan, we know it's not a group or entity.
                        isGroup.trySetValue(false);
                        isEntity.trySetValue(false);
                    }
                    return newIsPlan;
                } catch (StatusRuntimeException e) {
                    // Don't set the "isPlan" variable - we still don't know!
                    return false;
                }
            }
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
    public static boolean hasLimitedScope(@Nonnull final Collection<String> seedUuids) {
        return !CollectionUtils.isEmpty(seedUuids) && !seedUuids.contains(UI_REAL_TIME_MARKET_STR);
    }
}
