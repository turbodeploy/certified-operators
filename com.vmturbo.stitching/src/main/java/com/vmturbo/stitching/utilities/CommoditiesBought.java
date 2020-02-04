package com.vmturbo.stitching.utilities;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * A helper class that bundles list of bought commodities builders and volume id together. For
 * most cases, volume id will be null. But there is use case that an entity may buy multiple set
 * of CommoditiesBought from same provider.
 *
 * For example: cloud VM may buy multiple set of CommoditiesBought from same storage tier, depending
 * how may volumes are attached to it. Each CommoditiesBought has a list of bought commodities and
 * different volume ids.
 */
public class CommoditiesBought {

    private final List<CommodityDTO.Builder> boughtList;

    // Settings for the eligibility of an entity with respect to a particular provider.
    // Indicates the entity's eligibility to move across providers
    private Optional<Boolean> movable = Optional.empty();
    // Indicates the entity's eligibility to start on a provider
    private Optional<Boolean> startable = Optional.empty();
    // Indicates the entity's eligibility to scale on a provider
    private Optional<Boolean> scalable = Optional.empty();

    private Long volumeId;

    public CommoditiesBought(@Nonnull final List<CommodityDTO.Builder> boughtList, @Nullable Long volumeId) {
        this.boughtList = Objects.requireNonNull(boughtList);
        this.volumeId = volumeId;
    }

    public CommoditiesBought(@Nonnull final List<CommodityDTO.Builder> boughtList) {
        this.boughtList = Objects.requireNonNull(boughtList);
        this.volumeId = null;
    }

    @Nonnull
    public List<CommodityDTO.Builder> getBoughtList() {
        return boughtList;
    }

    @Nullable
    public Long getVolumeId() {
        return volumeId;
    }

    public void setVolumeId(@Nonnull Long volumeId) {
        this.volumeId = volumeId;
    }

    public Optional<Boolean> getMovable() {
        return movable;
    }

    public void setMovable(boolean movable) {
        this.movable = Optional.of(movable);
    }

    public Optional<Boolean> getStartable() {
        return startable;
    }

    public void setStartable(boolean startable) {
        this.startable = Optional.of(startable);
    }

    public Optional<Boolean> getScalable() {
        return scalable;
    }

    public void setScalable(boolean scalable) {
        this.scalable = Optional.of(scalable);
    }

    /**
     * Method checking whether the given CommoditiesBought matches this CommoditiesBought. There
     * may be multiple set of commodities bought from same provider. Currently, it is only used
     * by cloud VMs, so we match based on volumeId.
     *
     * Todo: if there are new use cases for buying multiple set of commodities bought, suppose
     * new fields is added to {@link CommoditiesBoughtFromProvider}, we may need to change the
     * matching logic based on that.
     *
     * @param commoditiesBought the CommoditiesBought to check matching with this one. It is
     * assumed that it is from the same provider. Provider check should be done before this.
     * @return true if the provided CommoditiesBought matches this one, otherwise false
     */
    public boolean match(@Nonnull CommoditiesBought commoditiesBought) {
        return Objects.equals(volumeId, commoditiesBought.getVolumeId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(boughtList, volumeId);
    }

    @Nonnull
    public CommoditiesBought deepCopy() {
        return new CommoditiesBought(boughtList.stream()
                .map(CommodityDTO.Builder::clone)
                .collect(Collectors.toList()), volumeId);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CommoditiesBought)) {
            return false;
        }

        @Nonnull CommoditiesBought that = (CommoditiesBought)obj;
        return Objects.equals(volumeId, that.volumeId) && boughtList.equals(that.boughtList);
    }
}
