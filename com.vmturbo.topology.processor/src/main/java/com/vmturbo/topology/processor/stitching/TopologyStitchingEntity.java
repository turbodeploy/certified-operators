package com.vmturbo.topology.processor.stitching;

import java.util.ArrayList;
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

    private final Map<StitchingEntity, List<CommodityDTO>> commoditiesBoughtByProvider = new IdentityHashMap<>();
    private final Set<StitchingEntity> consumers = Sets.newIdentityHashSet();
    private final List<CommoditySold> commoditiesSold = new ArrayList<>();

    public TopologyStitchingEntity(@Nonnull final StitchingEntityData stitchingEntityData) {
        this(stitchingEntityData.getEntityDtoBuilder(),
            stitchingEntityData.getOid(),
            stitchingEntityData.getTargetId());
    }

    public TopologyStitchingEntity(@Nonnull final EntityDTO.Builder entityBuilder,
                                   final long oid,
                                   final long targetId) {
        this.entityBuilder = Objects.requireNonNull(entityBuilder);
        this.oid = oid;
        this.targetId = targetId;
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
    public Set<StitchingEntity> getProviders() {
        return commoditiesBoughtByProvider.keySet();
    }

    @Override
    public Set<StitchingEntity> getConsumers() {
        return Collections.unmodifiableSet(consumers);
    }

    @Override
    public Stream<CommodityDTO> getCommoditiesSold() {
        return commoditiesSold.stream()
            .map(commoditySold -> commoditySold.sold);
    }

    @Override
    public Map<StitchingEntity, List<CommodityDTO>> getCommoditiesBoughtByProvider() {
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
    public Optional<List<CommodityDTO>> removeProvider(@Nonnull final StitchingEntity entity) {
        return Optional.ofNullable(commoditiesBoughtByProvider.remove(entity));
    }

    public void putProviderCommodities(@Nonnull final StitchingEntity entity,
                                       @Nonnull final List<CommodityDTO> commmoditiesBoughtFromProvider) {
        Preconditions.checkArgument(entity instanceof TopologyStitchingEntity);

        commoditiesBoughtByProvider.put(entity, commmoditiesBoughtFromProvider);
    }

    public Optional<List<CommodityDTO>> getProviderCommodities(@Nonnull final StitchingEntity entity) {
        return Optional.ofNullable(commoditiesBoughtByProvider.get(entity));
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
        return String.format("%s %s %s oid-%d targetId-%d numConsumers-%d numProviders-%d",
            getEntityType().name(), getLocalId(), getDisplayName(), getOid(), getTargetId(),
            consumers.size(), commoditiesBoughtByProvider.size());
    }

    public static class CommoditySold {
        /**
         * Get the commodity being sold.
         */
        @Nonnull
        public final CommodityDTO sold;

        /**
         * Don't use an Optional here so we don't have to allocate as many objects.
         */
        @Nullable
        public final TopologyStitchingEntity accesses;

        public CommoditySold(@Nonnull final CommodityDTO sold, @Nullable TopologyStitchingEntity accesses) {
            this.sold = Objects.requireNonNull(sold);
            this.accesses = accesses;
        }
    }
}