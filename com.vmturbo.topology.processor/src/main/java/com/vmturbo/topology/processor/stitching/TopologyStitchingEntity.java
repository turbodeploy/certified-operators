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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingMergeInformation;
import com.vmturbo.topology.processor.stitching.journal.StitchingEntitySemanticDiffer;

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

    // type of the probe this entity comes from, this is used to check if the entity comes from
    // cloud probe and differentiate between AWS and Azure, so entity is treated differently
    private final SDKProbeType probeType;

    private long lastUpdatedTime;

    /**
     * A list of {@link StitchingMergeInformation} for entities that were that were merged onto this entity.
     * For additional details, see {@link com.vmturbo.stitching.utilities.MergeEntities.MergeEntitiesDetails}.
     *
     * Note that for memory reasons, internally this is initialized to null but all public accessors
     * guarantee a non-null value (ie if the internal value is null but it is accessed publicly,
     * we will return an empty list).
     *
     * Use a list instead of a set even though the list should contain distinct elements because we
     * expect these to be generally quite small and this will help reduce the memory footprint.
     */
    private List<StitchingMergeInformation> mergeInformation;

    private final Map<StitchingEntity, List<CommodityDTO.Builder>> commoditiesBoughtByProvider = new IdentityHashMap<>();
    private final Set<StitchingEntity> consumers = Sets.newIdentityHashSet();
    private final List<CommoditySold> commoditiesSold = new ArrayList<>();

    private final Map<ConnectionType, Set<StitchingEntity>> connectedTo = new IdentityHashMap<>();

    public TopologyStitchingEntity(@Nonnull final StitchingEntityData stitchingEntityData) {
        this(stitchingEntityData.getEntityDtoBuilder(),
            stitchingEntityData.getOid(),
            stitchingEntityData.getTargetId(),
            stitchingEntityData.getLastUpdatedTime(),
            stitchingEntityData.getProbeType());
    }

    public TopologyStitchingEntity(@Nonnull final EntityDTO.Builder entityBuilder,
                                   final long oid,
                                   final long targetId,
                                   final long lastUpdatedTime) {
        this(entityBuilder, oid, targetId, lastUpdatedTime, null);
    }

    private TopologyStitchingEntity(@Nonnull final EntityDTO.Builder entityBuilder,
                                   final long oid,
                                   final long targetId,
                                   final long lastUpdatedTime,
                                   final SDKProbeType probeType) {
        this.entityBuilder = Objects.requireNonNull(entityBuilder);
        this.oid = oid;
        this.targetId = targetId;
        this.lastUpdatedTime = lastUpdatedTime;
        this.mergeInformation = null;
        this.probeType = probeType;
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

    @Nonnull
    @Override
    public EntityType getJournalableEntityType() {
        return getEntityType();
    }

    @Nonnull
    @Override
    public String removalDescription() {
        return String.format("REMOVED ENTITY\n\t[%s]",
            StitchingEntitySemanticDiffer.entityDescription(this));
    }

    @Nonnull
    @Override
    public StitchingEntity snapshot() {
        // Create a basic copy
        TopologyStitchingEntity copy = new TopologyStitchingEntity(entityBuilder.clone(), getOid(),
            targetId, lastUpdatedTime);

        // Copy consumers
        // Consumers do not need to be deep-copied because when performing a diff of consumers, we only
        // look at membership in this set, and don't need to check for changes to those consumers via
        // this set.
        copy.consumers.addAll(consumers);

        // Copy commodities sold
        commoditiesSold.forEach(sold -> copy.commoditiesSold.add(sold.deepCopy()));

        // Copy commoditiesBoughtByProvider
        commoditiesBoughtByProvider.forEach((provider, commodities) ->
            copy.commoditiesBoughtByProvider.put(provider, commodities.stream()
                .map(Builder::clone)
                .collect(Collectors.toList())));

        // copy connectedTo
        copy.connectedTo.putAll(connectedTo);

        // Copy merge information
        getMergeInformation().forEach(mergeInfo -> copy.addMergeInformation(
            new StitchingMergeInformation(mergeInfo.getOid(), mergeInfo.getTargetId())));

        return copy;
    }

    @Override
    public long getTargetId() {
        return targetId;
    }

    @Override
    public long getLastUpdatedTime() {
        return lastUpdatedTime;
    }

    public SDKProbeType getProbeType() {
        return probeType;
    }

    @Override
    public boolean updateLastUpdatedTime(final long updateTime) {
        if (updateTime > lastUpdatedTime) {
            this.lastUpdatedTime = updateTime;
            return true;
        }

        return false;
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
    public Set<StitchingEntity> getConnectedTo() {
        return Collections.unmodifiableSet(connectedTo.values().stream().flatMap(Set::stream)
                .collect(Collectors.toSet()));
    }

    public Map<ConnectionType, Set<StitchingEntity>> getConnectedToByType() {
        return connectedTo;
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

    public void addConnectedTo(@Nonnull final  ConnectionType connectionType,
            @Nonnull final StitchingEntity entity) {
        Preconditions.checkArgument(entity instanceof TopologyStitchingEntity);
        connectedTo.computeIfAbsent(connectionType, k -> Sets.newIdentityHashSet()).add(
                Objects.requireNonNull(entity));
    }

    public void setConnectedTo(@Nonnull final Map<ConnectionType, Set<StitchingEntity>> connectedTo) {
        this.connectedTo.clear();
        this.connectedTo.putAll(connectedTo);
    }

    @Override
    public String toString() {
        return String.format("(%s %s %s %s numConsumers-%d numProviders-%d)",
            getEntityType().name(), getLocalId(), getDisplayName(),
            StitchingMergeInformation.formatOidAndTarget(getOid(), getTargetId()),
            consumers.size(), commoditiesBoughtByProvider.size());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<StitchingMergeInformation> getMergeInformation() {
        if (mergeInformation == null) {
            return Collections.emptyList();
        } else {
            return Collections.unmodifiableList(mergeInformation);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean addMergeInformation(@Nonnull final StitchingMergeInformation mergeInfo) {
        if (mergeInformation == null) {
            mergeInformation = new ArrayList<>();
        }

        if (mergeInformation.contains(mergeInfo)) {
            return false;
        }

        mergeInformation.add(mergeInfo);
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addAllMergeInformation(@Nonnull final List<StitchingMergeInformation> mergeInfo) {
        mergeInfo.forEach(this::addMergeInformation);
    }

    @Override
    public boolean hasMergeInformation() {
        return !(mergeInformation == null);
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

        @Nonnull
        private CommoditySold deepCopy() {
            return new CommoditySold(sold.clone(), accesses);
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