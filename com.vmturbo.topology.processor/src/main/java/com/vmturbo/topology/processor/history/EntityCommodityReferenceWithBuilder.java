package com.vmturbo.topology.processor.history;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.stitching.EntityCommodityReference;

/**
 * A reference to a topology commodity with it's builder for later update.
 */
@Immutable
public class EntityCommodityReferenceWithBuilder extends EntityCommodityReference {
    private final CommoditySoldDTO.Builder soldBuilder;
    private final CommodityBoughtDTO.Builder boughtBuilder;

    /**
     * Construct the commodity reference.
     *
     * @param entityOid owner entity
     * @param commodityType commodity base type + key
     * @param soldBuilder sold commodity builder
     */
    public EntityCommodityReferenceWithBuilder(long entityOid, @Nonnull CommodityType commodityType,
                    @Nonnull CommoditySoldDTO.Builder soldBuilder) {
        super(entityOid, commodityType, null);
        this.soldBuilder = soldBuilder;
        this.boughtBuilder = null;
    }

    /**
     * Construct the commodity reference.
     *
     * @param entityOid owner entity
     * @param commodityType commodity base type + key
     * @param providerOid optional provider entity if bought, null if sold
     * @param boughtBuilder bought commodity builder
     */
    public EntityCommodityReferenceWithBuilder(long entityOid, @Nonnull CommodityType commodityType,
                    @Nullable Long providerOid, @Nonnull CommodityBoughtDTO.Builder boughtBuilder) {
        super(entityOid, commodityType, providerOid);
        Preconditions.checkArgument(providerOid != null, "Bought commodity should have a provider");
        this.soldBuilder = null;
        this.boughtBuilder = boughtBuilder;
    }

    /**
     * Construct the commodity reference.
     *
     * @param other another reference
     */
    protected EntityCommodityReferenceWithBuilder(@Nonnull EntityCommodityReferenceWithBuilder other) {
        super(other.getEntityOid(), other.getCommodityType(), other.getProviderOid());
        this.soldBuilder = other.soldBuilder;
        this.boughtBuilder = other.boughtBuilder;
    }

    public CommoditySoldDTO.Builder getSoldBuilder() {
        return soldBuilder;
    }

    public CommodityBoughtDTO.Builder getBoughtBuilder() {
        return boughtBuilder;
    }

}
