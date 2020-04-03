package com.vmturbo.stitching;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.common.protobuf.topology.TopologyDTO;

/**
 * Unique reference to an entity's commodity within a topology to use in collections.
 * Composite key consists of an entity oid, commodity type+key, provider oid for bought.
 * No volumes.
 */
@Immutable
public class EntityCommodityReference {
    private final long entityOid;
    private final TopologyDTO.CommodityType commodityType;
    private final Long providerOid;

    /**
     * Construct the commodity reference.
     *
     * @param entityOid owner entity
     * @param commodityType commodity base type + key
     * @param providerOid optional provider entity if bought, null if sold
     */
    public EntityCommodityReference(long entityOid, @Nonnull TopologyDTO.CommodityType commodityType,
                              @Nullable Long providerOid) {
        this.entityOid = entityOid;
        this.commodityType = commodityType;
        this.providerOid = providerOid;
    }

    public long getEntityOid() {
        return entityOid;
    }

    @Nonnull
    public TopologyDTO.CommodityType getCommodityType() {
        return commodityType;
    }

    @Nullable
    public Long getProviderOid() {
        return providerOid;
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityOid, commodityType, providerOid);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof EntityCommodityReference) {
            final EntityCommodityReference other = (EntityCommodityReference) obj;
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
