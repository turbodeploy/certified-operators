package com.vmturbo.repository.dto.cloud.commitment;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Class that encapsulates the {@link CommittedCommodityBought}.
 */
@JsonInclude(Include.NON_EMPTY)
public class CommittedCommodityBoughtRepoDTO {

    private CommodityType commodityType;
    private EntityType providerType;
    private Double capacity;

    /**
     * Constructor.
     *
     * @param committedCommodityBought the {@link CommittedCommodityBought}.
     */
    public CommittedCommodityBoughtRepoDTO(
            @Nonnull final CommittedCommodityBought committedCommodityBought) {
        if (committedCommodityBought.hasCapacity()) {
            this.capacity = committedCommodityBought.getCapacity();
        }
        if (committedCommodityBought.hasCommodityType()) {
            this.commodityType = committedCommodityBought.getCommodityType();
        }
        if (committedCommodityBought.hasProviderType()) {
            this.providerType = committedCommodityBought.getProviderType();
        }
    }

    /**
     * Creates an instance of {@link CommittedCommodityBought}.
     *
     * @return the {@link CommittedCommodityBought}
     */
    public CommittedCommodityBought createCommittedCommodityBought() {
        final CommittedCommodityBought.Builder committedCommodityBought =
                CommittedCommodityBought.newBuilder();
        if (this.capacity != null) {
            committedCommodityBought.setCapacity(this.capacity);
        }
        if (this.commodityType != null) {
            committedCommodityBought.setCommodityType(this.commodityType);
        }
        if (this.providerType != null) {
            committedCommodityBought.setProviderType(this.providerType);
        }
        return committedCommodityBought.build();
    }

    public CommodityType getCommodityType() {
        return commodityType;
    }

    public void setCommodityType(final CommodityType commodityType) {
        this.commodityType = commodityType;
    }

    public EntityType getProviderType() {
        return providerType;
    }

    public void setProviderType(final EntityType providerType) {
        this.providerType = providerType;
    }

    public Double getCapacity() {
        return capacity;
    }

    public void setCapacity(final Double capacity) {
        this.capacity = capacity;
    }

    @Override
    public String toString() {
        return CommittedCommodityBoughtRepoDTO.class.getSimpleName() + "{commodityType="
                + commodityType + ", providerType=" + providerType + ", capacity=" + capacity + '}';
    }
}
