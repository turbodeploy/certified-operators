package com.vmturbo.topology.processor.stitching;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.stitching.StitchingEntity;

/**
 * The concrete implementation of the {@link StitchingEntity} interface.
 */
public class TopologyStitchingEntity implements StitchingEntity {

    /**
     * The builder for the {@link }
     */
    private final EntityDTO.Builder entityBuilder;

    private final long oid;

    private final long targetId;

    private final long lastUpdatedTime;

    private final Map<StitchingEntity, List<CommodityDTO.Builder>> commoditiesBoughtByProvider = new IdentityHashMap<>();
    private final Set<StitchingEntity> consumers = Sets.newIdentityHashSet();
    private final List<CommoditySold> commoditiesSold = new ArrayList<>();

    public TopologyStitchingEntity(@Nonnull final StitchingEntityData stitchingEntityData) {
        this(stitchingEntityData.getEntityDtoBuilder(),
            stitchingEntityData.getOid(),
            stitchingEntityData.getTargetId(),
            stitchingEntityData.getLastUpdatedTime());
    }

    public TopologyStitchingEntity(@Nonnull final EntityDTO.Builder entityBuilder,
                                   final long oid,
                                   final long targetId,
                                   final long lastUpdatedTime) {
        this.entityBuilder = Objects.requireNonNull(entityBuilder);
        this.oid = oid;
        this.targetId = targetId;
        this.lastUpdatedTime = lastUpdatedTime;
    }

    @Nonnull
    @Override
    public EntityDTO.Builder getEntityBuilder() {
        return entityBuilder;
    }

    @Override
    public long getOid() {
        return oid;
    }

    @Override
    public long getTargetId() {
        return targetId;
    }

    @Override
    public long getLastUpdatedTime() {
        return lastUpdatedTime;
    }

    @Override
    public Set<StitchingEntity> getProviders() {
        return commoditiesBoughtByProvider.keySet();
    }

    @Override
    public Set<StitchingEntity> getConsumers() {
        return Collections.unmodifiableSet(consumers);
    }

    @Override
    public Stream<CommodityDTO.Builder> getCommoditiesSold() {
        return commoditiesSold.stream()
            .map(commoditySold -> commoditySold.sold);
    }

    @Override
    public void addCommoditySold(@Nonnull final CommodityDTO.Builder commoditySold,
                                 @Nonnull final Optional<StitchingEntity> accesses) {
        final CommoditySold newCommodity = new CommoditySold(commoditySold,
            (TopologyStitchingEntity)accesses.orElse(null));

        commoditiesSold.add(newCommodity);
    }

    @Override
    public Map<StitchingEntity, List<CommodityDTO.Builder>> getCommoditiesBoughtByProvider() {
        return commoditiesBoughtByProvider;
    }

    public List<CommoditySold> getTopologyCommoditiesSold() {
        return commoditiesSold;
    }

    public Stream<TopologyStitchingEntity> getTopologyProviders() {
        return commoditiesBoughtByProvider.keySet().stream().map(e -> (TopologyStitchingEntity)e);
    }

    public Stream<TopologyStitchingEntity> getTopologyConsumers() {
        return consumers.stream().map(entity -> (TopologyStitchingEntity)entity);
    }

    public boolean removeConsumer(@Nonnull final StitchingEntity entity) {
        return consumers.remove(Objects.requireNonNull(entity));
    }

    public void addConsumer(@Nonnull final StitchingEntity entity) {
        Preconditions.checkArgument(entity instanceof TopologyStitchingEntity);

        consumers.add(Objects.requireNonNull(entity));
    }

    @Override
    public boolean hasConsumer(@Nonnull final StitchingEntity entity) {
        return consumers.contains(entity);
    }

    @Override
    public Optional<List<CommodityDTO.Builder>> removeProvider(@Nonnull final StitchingEntity entity) {
        return Optional.ofNullable(commoditiesBoughtByProvider.remove(entity));
    }

    public void putProviderCommodities(@Nonnull final StitchingEntity entity,
                                       @Nonnull final List<CommodityDTO.Builder> commmoditiesBoughtFromProvider) {
        Preconditions.checkArgument(entity instanceof TopologyStitchingEntity);

        commoditiesBoughtByProvider.put(entity, commmoditiesBoughtFromProvider);
    }

    public Optional<List<CommodityDTO.Builder>> getProviderCommodities(@Nonnull final StitchingEntity entity) {
        return Optional.ofNullable(commoditiesBoughtByProvider.get(entity));
    }

    public void setCommoditiesSold(@Nonnull final Collection<CommoditySold> commoditiesSold) {
        this.commoditiesSold.clear();
        this.commoditiesSold.addAll(commoditiesSold);
    }

    @Override
    public boolean hasProvider(@Nonnull final StitchingEntity entity) {
        return commoditiesBoughtByProvider.containsKey(entity);
    }

    public void clearConsumers() {
        consumers.clear();
    }

    public void clearProviders() {
        commoditiesBoughtByProvider.clear();
    }

    @Override
    public String toString() {
        return String.format("(%s %s %s oid-%d targetId-%d numConsumers-%d numProviders-%d)",
            getEntityType().name(), getLocalId(), getDisplayName(), getOid(), getTargetId(),
            consumers.size(), commoditiesBoughtByProvider.size());
    }

    public static class CommoditySold {
        /**
         * Get the commodity being sold.
         */
        @Nonnull
        public final CommodityDTO.Builder sold;

        /**
         * Don't use an Optional here so we don't have to allocate as many objects.
         *
         * For DSPM_ACCESS commodity this is the PhysicalMachine associated with the Storage
         * that sells this commodity. For DATASTORE commodity this is the Storage associated
         * with the PhysicalMachine that sells this commodity.
         *
         * TODO: Appropriately update accesses when stitching mutates the topology.
         */
        @Nullable
        public final TopologyStitchingEntity accesses;

        public CommoditySold(@Nonnull final CommodityDTO.Builder sold, @Nullable TopologyStitchingEntity accesses) {
            this.sold = Objects.requireNonNull(sold);
            this.accesses = accesses;
        }

        @Override
        public int hashCode() {
            return Objects.hash(sold, accesses);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof CommoditySold)) {
                return false;
            }

            @Nonnull CommoditySold that = (CommoditySold)obj;
            return (accesses == that.accesses && sold.equals(that.sold));
        }
    }
}