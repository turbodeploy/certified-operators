package com.vmturbo.stitching;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeView;

/**
 * Unique reference to an entity's commodity within a topology to use in collections.
 * Composite key consists of an entity oid, commodity type+key, provider oid for bought.
 */
@Immutable
public class EntityCommodityReference implements Serializable {
    private static final long NO_PROVIDER = 0L;

    private final long entityOid;
    private final CommodityTypeView commodityType;
    private final long providerOid;

    /**
     * Construct the commodity reference.
     *
     * @param entityOid owner entity
     * @param commodityType commodity base type + key
     * @param providerOid optional provider entity if bought, null if sold
     */
    public EntityCommodityReference(long entityOid, @Nonnull CommodityTypeView commodityType,
                              @Nullable Long providerOid) {
        this.entityOid = entityOid;
        this.commodityType = commodityType;
        this.providerOid = providerOid == null ? NO_PROVIDER : providerOid;
    }

    public long getEntityOid() {
        return entityOid;
    }

    @Nonnull
    public CommodityTypeView getCommodityType() {
        return commodityType;
    }

    @Nullable
    public Long getProviderOid() {
        return providerOid == NO_PROVIDER ? null : providerOid;
    }

    /**
     * Gets the reference to live topology entity.
     *
     * @param liveTopologyEntityOidGetter The function which gets an entity oid and returns live
     *                                    entity oid.
     * @return The reference to live topology entity.
     */
    @Nonnull
    public EntityCommodityReference getLiveTopologyCommodityReference(
            @Nonnull Function<Long, Optional<Long>> liveTopologyEntityOidGetter) {
        final long entityId = getEntityOid();
        final Long providerId = getProviderOid();

        // Find the live topology entity id for the plan entity
        final long liveEntityId = liveTopologyEntityOidGetter.apply(entityId).orElse(entityId);
        final Long liveProviderId =
            liveTopologyEntityOidGetter.apply(providerId).orElse(providerId);

        // If the live topology ids are different create a reference with those
        if (entityId != liveEntityId || !Objects.equals(providerId, liveProviderId)) {
            return new EntityCommodityReference(liveEntityId, getCommodityType(),
                liveProviderId);
        }

        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityOid, commodityType, providerOid);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof EntityCommodityReference) {
            final EntityCommodityReference other = (EntityCommodityReference)obj;
            return (entityOid == other.entityOid
                        && Objects.equals(commodityType, other.commodityType)
                        && Objects.equals(providerOid, other.providerOid));
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " [entityOid=" + entityOid
               + ", providerOid="
               + providerOid
               + ", commodityType="
               + commodityType
               + "]";
    }

}
