package com.vmturbo.repository.dto;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

/**
 * Uses in ServiceEntityRepoDTO to solve Commodities bought without providers issue.
 * It has the same structure as CommoditiesBoughtFromProvider in TopologyEntityDTO,
 * It helps simplify the conversion from TopologyEntityDTO to ServiceEntityRepoDTO.
 */
@JsonInclude(Include.NON_EMPTY)
public class CommoditiesBoughtRepoFromProviderDTO {
    private Long providerId;

    private Integer providerEntityType;

    private List<CommodityBoughtRepoDTO> commodityBoughtRepoDTOs;

    public Long getProviderId() {
        return providerId;
    }

    public void setProviderId(@Nullable final Long providerId) {
        this.providerId = providerId;
    }

    public Integer getProviderEntityType() {
        return providerEntityType;
    }

    public void setProviderEntityType(@Nullable final Integer providerEntityType) {
        this.providerEntityType = providerEntityType;
    }

    public List<CommodityBoughtRepoDTO> getCommodityBoughtRepoDTOs() {
        return commodityBoughtRepoDTOs;
    }

    public void setCommodityBoughtRepoDTOs(List<CommodityBoughtRepoDTO> commodityBoughtRepoDTOs) {
        this.commodityBoughtRepoDTOs = commodityBoughtRepoDTOs;
    }

    @Override
    public String toString() {
        return "CommoditiesBoughtRepoFromProviderDTO{" +
                "providerId=" + providerId +
                ", providerEntityType=" + providerEntityType +
                ", commodityBoughtRepoDTOs=" + commodityBoughtRepoDTOs +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final CommoditiesBoughtRepoFromProviderDTO that = (CommoditiesBoughtRepoFromProviderDTO) o;

        if (!providerId.equals(that.providerId)) return false;
        if (!providerEntityType.equals(that.providerEntityType)) return false;
        return commodityBoughtRepoDTOs.equals(that.commodityBoughtRepoDTOs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(providerId, providerEntityType, commodityBoughtRepoDTOs);
    }
}
