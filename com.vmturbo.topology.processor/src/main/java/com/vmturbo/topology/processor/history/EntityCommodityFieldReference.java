package com.vmturbo.topology.processor.history;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.stitching.EntityCommodityReference;

/**
 * Reference to a historical field within a topology commodity (used or peak).
 */
@Immutable
public class EntityCommodityFieldReference extends EntityCommodityReference {
    private final CommodityField field;
    private volatile EntityCommodityFieldReference liveEntityReference;

    /**
     * Construct the field reference from a commodity reference and field marker.
     *
     * @param commRef commodity reference
     * @param field commodity's field
     */
    public EntityCommodityFieldReference(@Nonnull EntityCommodityReference commRef,
                    @Nonnull CommodityField field) {
        super(commRef.getEntityOid(), commRef.getCommodityType(), commRef.getProviderOid());
        this.field = field;
    }

    /**
     * Construct the field reference for a sold commodity.
     *
     * @param entityOid entity oid
     * @param commodityType commodity type
     * @param field commodity's field
     */
    public EntityCommodityFieldReference(long entityOid, @Nonnull CommodityType commodityType,
                    @Nonnull CommodityField field) {
        super(entityOid, commodityType, null);
        this.field = field;
    }

    /**
     * Construct the field reference for a bought commodity.
     *
     * @param entityOid entity oid
     * @param commodityType commodity type
     * @param providerOid commodity provider
     * @param field commodity's field
     */
    public EntityCommodityFieldReference(long entityOid, @Nonnull CommodityType commodityType,
                    @Nullable Long providerOid, @Nonnull CommodityField field) {
        super(entityOid, commodityType, providerOid);
        this.field = field;
    }

    @Nonnull
    public CommodityField getField() {
        return field;
    }

    /**
     * Returns the commodity field reference in the live topology.
     *
     * @param context History aggregation context.
     * @return The commodity field reference in live topology.
     */
    @Nonnull
    public EntityCommodityFieldReference getLiveTopologyFieldReference(
                                                    @Nonnull HistoryAggregationContext context) {
        if (liveEntityReference == null) {
            synchronized (this) {
                if (liveEntityReference == null) {
                    liveEntityReference = buildLiveTopologyReference(context);
                }
            }
        }
        return liveEntityReference;
    }

    @Nonnull
    private EntityCommodityFieldReference buildLiveTopologyReference(
                                                    @Nonnull HistoryAggregationContext context) {
        // If this is live topology return this
        if (!context.isPlan()) {
            return this;
        }

        final long entityId = getEntityOid();
        final Long providerId = getProviderOid();

        // Find the live topology entity id for the plan entity
        final long liveEntityId = context.getClonedFromEntityOid(entityId).orElse(entityId);
        final Long liveProviderId = context.getClonedFromEntityOid(providerId).orElse(providerId);

        // If the live topology ids are different create a reference with those
        if (entityId != liveEntityId || !Objects.equals(providerId, liveProviderId)) {
            return new EntityCommodityFieldReference(liveEntityId, getCommodityType(),
                liveProviderId, field);
        }

        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), field);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof EntityCommodityFieldReference)) {
            return false;
        }
        return super.equals(obj) && Objects.equals(field, ((EntityCommodityFieldReference)obj).getField());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " [field=" + field
               + ", oid="
               + getEntityOid()
               + ", providerOid="
               + getProviderOid()
               + ", commodityType="
               + getCommodityType()
               + "]";
    }

}
