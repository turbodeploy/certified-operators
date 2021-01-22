package com.vmturbo.stitching.utilities;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.builders.CommodityBuilderIdentifier;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * A helper class that bundles list of bought commodities builders.
 */
public class CommoditiesBought {

    private final List<CommodityDTO.Builder> boughtList;
    // will be created for entities being stitched
    private volatile Set<CommodityBuilderIdentifier> identity;

    // Settings for the eligibility of an entity with respect to a particular provider.
    // Indicates the entity's eligibility to move across providers
    private Optional<Boolean> movable = Optional.empty();
    // Indicates the entity's eligibility to start on a provider
    private Optional<Boolean> startable = Optional.empty();
    // Indicates the entity's eligibility to scale on a provider
    private Optional<Boolean> scalable = Optional.empty();

    public CommoditiesBought(@Nonnull final List<CommodityDTO.Builder> boughtList) {
        this.boughtList = Objects.requireNonNull(boughtList);
    }

    @Nonnull
    public List<CommodityDTO.Builder> getBoughtList() {
        return boughtList;
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

    @Override
    public int hashCode() {
        return Objects.hash(identity());
    }

    @Nonnull
    public CommoditiesBought deepCopy() {
        return new CommoditiesBought(boughtList.stream()
                .map(CommodityDTO.Builder::clone)
                .collect(Collectors.toList()));
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CommoditiesBought)) {
            return false;
        }

        CommoditiesBought that = (CommoditiesBought)obj;
        return Objects.equals(identity(), that.identity());
    }

    private Set<CommodityBuilderIdentifier> identity() {
        if (identity == null) {
            synchronized (this) {
                if (identity == null) {
                    identity = boughtList.stream()
                                    .map(comm -> new CommodityBuilderIdentifier(
                                                    comm.getCommodityType(), comm.getKey()))
                                    .collect(Collectors.toSet());
                }
            }
        }
        return identity;
    }
}
