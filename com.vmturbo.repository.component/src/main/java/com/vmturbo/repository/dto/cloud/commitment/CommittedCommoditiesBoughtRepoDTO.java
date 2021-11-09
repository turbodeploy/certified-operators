package com.vmturbo.repository.dto.cloud.commitment;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommoditiesBought;

/**
 * Class that encapsulates the {@link CommittedCommoditiesBought}.
 */
@JsonInclude(Include.NON_EMPTY)
public class CommittedCommoditiesBoughtRepoDTO {

    private final List<CommittedCommodityBoughtRepoDTO> commoditiesBought;

    /**
     * Constructor.
     *
     * @param commoditiesBought the list of {@link CommittedCommoditiesBought}.
     */
    public CommittedCommoditiesBoughtRepoDTO(
            @Nonnull final CommittedCommoditiesBought commoditiesBought) {
        this.commoditiesBought = commoditiesBought.getCommodityList().stream().map(
                CommittedCommodityBoughtRepoDTO::new).collect(Collectors.toList());
    }

    /**
     * Creates an instance of {@link CommittedCommoditiesBought}.
     *
     * @return the {@link CommittedCommoditiesBought}
     */
    @Nonnull
    public CommittedCommoditiesBought createCommittedCommoditiesBought() {
        final CommittedCommoditiesBought.Builder committedCommoditiesBought =
                CommittedCommoditiesBought.newBuilder();
        commoditiesBought.forEach(
                c -> committedCommoditiesBought.addCommodity(c.createCommittedCommodityBought()));
        return committedCommoditiesBought.build();
    }

    public List<CommittedCommodityBoughtRepoDTO> getCommoditiesBought() {
        return commoditiesBought;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final CommittedCommoditiesBoughtRepoDTO that = (CommittedCommoditiesBoughtRepoDTO)o;
        return Objects.equals(commoditiesBought, that.commoditiesBought);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commoditiesBought);
    }

    @Override
    public String toString() {
        return CommittedCommoditiesBoughtRepoDTO.class.getSimpleName() + "{commoditiesBought="
                + commoditiesBought + '}';
    }
}
