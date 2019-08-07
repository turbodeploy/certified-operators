package com.vmturbo.topology.processor.history;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;

/**
 * Reference to a historical field within a topology commodity (used or peak).
 */
@Immutable
public class EntityCommodityFieldReference extends EntityCommodityReferenceWithBuilder {
    private final CommodityField field;

    /**
     * Construct the field reference from a commodity reference and field marker.
     *
     * @param commRef commodity reference
     * @param field commodity's field
     */
    public EntityCommodityFieldReference(@Nonnull EntityCommodityReferenceWithBuilder commRef,
                    @Nonnull CommodityField field) {
        super(commRef);
        this.field = field;
    }

    /**
     * Construct the field reference for a sold commodity.
     *
     * @param entityOid entity oid
     * @param commodityType commodity type
     * @param soldBuilder sold commodity builder
     * @param field commodity's field
     */
    public EntityCommodityFieldReference(long entityOid, @Nonnull CommodityType commodityType,
                    @Nonnull CommoditySoldDTO.Builder soldBuilder,
                    @Nonnull CommodityField field) {
        super(entityOid, commodityType, soldBuilder);
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
                    @Nullable Long providerOid, @Nonnull CommodityBoughtDTO.Builder boughtBuilder,
                    @Nonnull CommodityField field) {
        super(entityOid, commodityType, providerOid, boughtBuilder);
        this.field = field;
    }

    @Nonnull
    public CommodityField getField() {
        return field;
    }

    /**
     * Get the builder for historical used values (whether the commodity is bought or sold).
     *
     * @return historical values used builder
     */
    @Nonnull
    public HistoricalValues.Builder getHistoricalUsedBuilder() {
        return getProviderOid() == null ? getSoldBuilder().getHistoricalUsedBuilder()
                        : getBoughtBuilder().getHistoricalUsedBuilder();
    }

    /**
     * Get the builder for historical peak values (whether the commodity is bought or sold).
     *
     * @return historical values peak builder
     */
    @Nonnull
    public HistoricalValues.Builder getHistoricalPeakBuilder() {
        return getProviderOid() == null ? getSoldBuilder().getHistoricalPeakBuilder()
                        : getBoughtBuilder().getHistoricalPeakBuilder();
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
               + ", commodityType="
               + getCommodityType()
               + ", providerOid="
               + getProviderOid()
               + "]";
    }

}
