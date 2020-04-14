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
     * @param boughtBuilder bought commodity builder
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
