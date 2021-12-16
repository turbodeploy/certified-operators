package com.vmturbo.topology.processor.stitching;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
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

import org.apache.commons.lang.StringUtils;

import com.vmturbo.common.protobuf.topology.StitchingErrors;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.EntityPipelineErrors.StitchingErrorCode;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingMergeInformation;
import com.vmturbo.stitching.utilities.CommoditiesBought;
import com.vmturbo.topology.processor.stitching.journal.StitchingEntitySemanticDiffer;

/**
 * The concrete implementation of the {@link StitchingEntity} interface.
 *
 * Many fields in this class are initialized to empty because creating default collections
 * on millions of entities increases memory usage dramatically when many times these
 * data structures will be empty or have very few values.
 */
public class TopologyStitchingEntity implements StitchingEntity {

    /**
     * Capacity for a map we expect to contain very few elements.
     */
    private static final int MINI_MAP_CAPACITY = 4;

    /**
     * Capacity for a map we expect to contain only a small number of elements.
     */
    private static final int SMALL_MAP_CAPACITY = 8;

    /**
     * Value for connections maps before they are initialized.
     */
    private static final Map<ConnectionType, Set<StitchingEntity>> UNINITIALIZED_CONNECTIONS =
        Collections.emptyMap();

    /**
     * Value for consumers map before it is initialized.
     */
    private static final Set<StitchingEntity> UNINITIALIZED_CONSUMERS = Collections.emptySet();

    /**
     * The builder for the {@link }
     */
    private final EntityDTO.Builder entityBuilder;

    private final long oid;

    private final long targetId;

    private long lastUpdatedTime;

    private final boolean isStale;

    /**
     * The errors encountered by this entity during any part of stitching.
     */
    private StitchingErrors stitchingErrors;

    /**
     * A set of {@link StitchingMergeInformation} for entities that were that were merged onto this entity.
     * For additional details, see {@link com.vmturbo.stitching.utilities.MergeEntities.MergeEntitiesDetails}.
     *
     * Note that for memory reasons, internally this is initialized to null but all public accessors
     * guarantee a non-null value (ie if the internal value is null but it is accessed publicly,
     * we will return an empty set).
     *
     * Use a set instead of a list since there may be hundreds of StitchingMergeInformation
     * for shared cloud entities if there are hundreds of cloud targets. This speeds up adding new
     * StitchingMergeInformation with the price of a slightly higher memory footprint.
     */
    private Set<StitchingMergeInformation> mergeInformation;

    private final Map<StitchingEntity, List<CommoditiesBought>> commodityBoughtListByProvider =
        miniIdentityHashMap();
    private final List<CommoditySold> commoditiesSold = new ArrayList<>();

    private Set<StitchingEntity> consumers = UNINITIALIZED_CONSUMERS;

    private Map<ConnectionType, Set<StitchingEntity>> connectedTo = UNINITIALIZED_CONNECTIONS;

    private Map<ConnectionType, Set<StitchingEntity>> connectedFrom = UNINITIALIZED_CONNECTIONS;

    /**
     * Indicates that this is a proxy object that should be removed if it doesn't get stitched.
     */
    private final boolean removeIfUnstitched;

    public TopologyStitchingEntity(@Nonnull final StitchingEntityData stitchingEntityData) {
        this(stitchingEntityData.getEntityDtoBuilder(),
            stitchingEntityData.getOid(),
            stitchingEntityData.getTargetId(),
            stitchingEntityData.getLastUpdatedTime(),
            stitchingEntityData.isStale());
    }

    /**
     * Construct a stitching entity.
     *
     * @param entityBuilder entity builder
     * @param oid entity identifier
     * @param targetId target identifier
     * @param lastUpdatedTime last updated time
     */
    public TopologyStitchingEntity(@Nonnull final EntityDTO.Builder entityBuilder,
                                   final long oid,
                                   final long targetId,
                                   final long lastUpdatedTime) {
        this(entityBuilder, oid, targetId, lastUpdatedTime, false);
    }

    /**
     * Construct a stitching entity.
     *
     * @param entityBuilder entity builder
     * @param oid entity identifier
     * @param targetId target identifier
     * @param lastUpdatedTime last updated time
     * @param isStale whether the entity is considered outdated
     */
    public TopologyStitchingEntity(@Nonnull final EntityDTO.Builder entityBuilder,
                                   final long oid,
                                   final long targetId,
                                   final long lastUpdatedTime,
                                   boolean isStale) {
        this.entityBuilder = Objects.requireNonNull(entityBuilder);
        this.oid = oid;
        this.targetId = targetId;
        this.lastUpdatedTime = lastUpdatedTime;
        this.mergeInformation = null;
        this.isStale = isStale;
        removeIfUnstitched = !entityBuilder.getKeepStandalone()
            && EntityOrigin.PROXY == entityBuilder.getOrigin();
    }

    @Nonnull
    @Override
    public Collection<String> getPropertyValues(@Nonnull String name) {
        return entityBuilder.getEntityPropertiesList().stream()
            .filter(p -> Objects.equals(p.getName(), name))
            .map(EntityProperty::getValue)
            .filter(StringUtils::isNotBlank)
            .collect(Collectors.toSet());
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
    public StitchingErrors getStitchingErrors() {
        return stitchingErrors == null ? StitchingErrors.none() : stitchingErrors ;
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
    public String additionDescription() {
        return String.format("ADDED ENTITY\n\t[%s]",
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
        if (consumers != UNINITIALIZED_CONSUMERS) {
            copy.consumers = smallIdentityHashSet();
            copy.consumers.addAll(consumers);
        }

        // Copy commodities sold
        commoditiesSold.forEach(sold -> copy.commoditiesSold.add(sold.deepCopy()));

        // Copy commodityBoughtListByProvider
        commodityBoughtListByProvider.forEach((provider, commodityBoughtList) ->
            copy.commodityBoughtListByProvider.put(provider, commodityBoughtList.stream()
                .map(CommoditiesBought::deepCopy)
                .collect(Collectors.toList())));

        // copy connectedTo and connectedFrom
        if (connectedTo != UNINITIALIZED_CONNECTIONS) {
            copy.connectedTo = new EnumMap<>(ConnectionType.class);
            copy.connectedTo.putAll(connectedTo);
        }

        if (connectedFrom != UNINITIALIZED_CONNECTIONS) {
            copy.connectedFrom = new EnumMap<>(ConnectionType.class);
            copy.connectedFrom.putAll(connectedFrom);
        }

        // Copy merge information
        getMergeInformation().forEach(mergeInfo -> copy
            .addMergeInformation(new StitchingMergeInformation(mergeInfo.getOid(),
                mergeInfo.getTargetId(),
                mergeInfo.getError(),
                mergeInfo.getVendorId(),
                mergeInfo.getOrigin())));

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

    @Override
    public void recordError(@Nonnull final StitchingErrorCode errorCode) {
        if (this.stitchingErrors == null) {
            stitchingErrors = new StitchingErrors();
        }

        this.stitchingErrors.add(errorCode);
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
        return commodityBoughtListByProvider.keySet();
    }

    @Override
    public Set<StitchingEntity> getConsumers() {
        return Collections.unmodifiableSet(consumers);
    }

    @Override
    public Map<ConnectionType, Set<StitchingEntity>> getConnectedToByType() {
        return Collections.unmodifiableMap(connectedTo);
    }

    @Override
    public Map<ConnectionType, Set<StitchingEntity>> getConnectedFromByType() {
        return Collections.unmodifiableMap(connectedFrom);
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
    public Map<StitchingEntity, List<CommoditiesBought>> getCommodityBoughtListByProvider() {
        return commodityBoughtListByProvider;
    }

    /**
     * Returns an optional of {@link CommoditiesBought} from the given provider {@link StitchingEntity}.
     *
     * @param provider the provider from which to find commodity bought
     * @param commoditiesBought the {@link CommoditiesBought} instance used to match the
     * CommoditiesBought on this entity
     * @return an optional of commodities bought from the given provider. If there is no commodities
     * bought from the given provider, return an empty Optional instance.
     */
    @Override
    public Optional<CommoditiesBought> getMatchingCommoditiesBought(@Nonnull StitchingEntity provider,
                                                                    @Nonnull CommoditiesBought commoditiesBought) {
        // TODO: After the volume model change in OM-62262, there will not be multiple sets of
        //   CommoditiesBought from the same provider. As a result, the value of the
        //   commodityBoughtListByProvider map should contain only one element.
        //   We should create a follow up task to change:
        //       Map<StitchingEntity, List<CommoditiesBought>> commodityBoughtListByProvider
        //   to:
        //       Map<StitchingEntity, CommoditiesBought> commoditiesBoughtByProvider
        //   and then change all places referencing this variable.
        return Optional
            .ofNullable(commodityBoughtListByProvider.get(provider))
            .flatMap(cbList -> cbList.stream()
                .filter(Objects::nonNull)
                .findFirst());
    }

    public List<CommoditySold> getTopologyCommoditiesSold() {
        return commoditiesSold;
    }

    public Stream<TopologyStitchingEntity> getTopologyProviders() {
        return commodityBoughtListByProvider.keySet().stream().map(e -> (TopologyStitchingEntity)e);
    }

    public Stream<TopologyStitchingEntity> getTopologyConsumers() {
        return consumers.stream().map(entity -> (TopologyStitchingEntity)entity);
    }

    public boolean removeConsumer(@Nonnull final StitchingEntity entity) {
        return consumers != UNINITIALIZED_CONSUMERS && consumers.remove(Objects.requireNonNull(entity));
    }

    public void addConsumer(@Nonnull final StitchingEntity entity) {
        Preconditions.checkArgument(entity instanceof TopologyStitchingEntity);

        if (consumers == UNINITIALIZED_CONSUMERS) {
            consumers = smallIdentityHashSet();
        }
        consumers.add(Objects.requireNonNull(entity));
    }

    @Override
    public boolean hasConsumer(@Nonnull final StitchingEntity entity) {
        return consumers.contains(entity);
    }

    @Override
    public Optional<List<CommoditiesBought>> removeProvider(@Nonnull final StitchingEntity entity) {
        return Optional.ofNullable(commodityBoughtListByProvider.remove(entity));
    }

    @Override
    public boolean removeConnectedTo(@Nonnull final StitchingEntity connectedTo, @Nonnull final ConnectionType type) {
        if (this.connectedTo == UNINITIALIZED_CONNECTIONS) {
            return false;
        }

        final Set<StitchingEntity> connectedToOfType = this.connectedTo.get(type);
        if (connectedToOfType != UNINITIALIZED_CONNECTIONS) {
            return connectedToOfType.remove(connectedTo);
        } else {
            return false;
        }
    }

    @Override
    public boolean removeConnectedFrom(@Nonnull StitchingEntity connectedFrom,
                                       @Nonnull ConnectionType type) {
        if (this.connectedFrom == UNINITIALIZED_CONNECTIONS) {
            return false;
        }

        final Set<StitchingEntity> connectedFromOfType = this.connectedFrom.get(type);
        if (connectedFromOfType != null) {
            return connectedFromOfType.remove(connectedFrom);
        } else {
            return false;
        }
    }

    public void addProviderCommodityBought(@Nonnull final StitchingEntity entity,
                                           @Nonnull final CommoditiesBought commoditiesBought) {
        Preconditions.checkArgument(entity instanceof TopologyStitchingEntity);

        commodityBoughtListByProvider.computeIfAbsent(entity, k -> new ArrayList<>())
            .add(commoditiesBought);
    }

    public void setCommoditiesSold(@Nonnull final Collection<CommoditySold> commoditiesSold) {
        this.commoditiesSold.clear();
        this.commoditiesSold.addAll(commoditiesSold);
    }

    @Override
    public boolean hasProvider(@Nonnull final StitchingEntity entity) {
        return commodityBoughtListByProvider.containsKey(entity);
    }

    public void clearConsumers() {
        consumers = UNINITIALIZED_CONSUMERS;
    }

    public void clearProviders() {
        commodityBoughtListByProvider.clear();
    }

    public void addConnectedTo(@Nonnull final ConnectionType connectionType,
                               @Nonnull final StitchingEntity entity) {
        Preconditions.checkArgument(entity instanceof TopologyStitchingEntity);
        if (connectedTo == UNINITIALIZED_CONNECTIONS) {
            connectedTo = new EnumMap<>(ConnectionType.class);
        }

        connectedTo.computeIfAbsent(connectionType, k -> smallIdentityHashSet()).add(entity);
    }

    @Override
    public void addConnectedTo(@Nonnull final ConnectionType connectionType,
                               @Nonnull final Set<StitchingEntity> entities) {
        if (connectedTo == UNINITIALIZED_CONNECTIONS) {
            connectedTo = new EnumMap<>(ConnectionType.class);
        }

        connectedTo.computeIfAbsent(connectionType, k -> smallIdentityHashSet()).addAll(entities);
    }

    public void addConnectedFrom(@Nonnull final ConnectionType connectionType,
                                 @Nonnull final StitchingEntity entity) {
        Preconditions.checkArgument(entity instanceof TopologyStitchingEntity);
        if (connectedFrom == UNINITIALIZED_CONNECTIONS) {
            connectedFrom = new EnumMap<>(ConnectionType.class);
        }

        connectedFrom.computeIfAbsent(connectionType, k -> smallIdentityHashSet()).add(entity);
    }

    public void addConnectedFrom(@Nonnull final ConnectionType connectionType,
                                 @Nonnull final Set<StitchingEntity> entities) {
        if (connectedFrom == UNINITIALIZED_CONNECTIONS) {
            connectedFrom = new EnumMap<>(ConnectionType.class);
        }

        connectedFrom.computeIfAbsent(connectionType, k -> smallIdentityHashSet()).addAll(entities);
    }

    public boolean removeConnectedFrom(@Nonnull final ConnectionType connectionType,
                                       @Nonnull final StitchingEntity entity) {
        Preconditions.checkArgument(entity instanceof TopologyStitchingEntity);
        if (connectedFrom == UNINITIALIZED_CONNECTIONS) {
            return false;
        }

        Set<StitchingEntity> connectedFromEntities = connectedFrom.get(connectionType);
        if (connectedFromEntities == null) {
            return false;
        }
        return connectedFromEntities.remove(entity);
    }

    /**
     * Clear connected to relationship.
     */
    public void clearConnectedTo() {
        connectedTo = UNINITIALIZED_CONNECTIONS;
    }

    /**
     * Clear connected from relationship.
     */
    public void clearConnectedFrom() {
        connectedFrom = UNINITIALIZED_CONNECTIONS;
    }

    @Override
    public String toString() {
        return String.format("(%s %s %s %s numConsumers-%d numProviders-%d)",
            getEntityType().name(), getLocalId(), getDisplayName(),
            StitchingMergeInformation.formatOidAndTarget(getOid(), getTargetId()),
            getConsumers().size(), commodityBoughtListByProvider.size());
    }

    /**
     * {@inheritDoc}
     * @return
     */
    @Override
    public Collection<StitchingMergeInformation> getMergeInformation() {
        if (mergeInformation == null) {
            return Collections.emptySet();
        } else {
            return Collections.unmodifiableSet(mergeInformation);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean addMergeInformation(@Nonnull final StitchingMergeInformation mergeInfo) {
        if (mergeInformation == null) {
            mergeInformation = new HashSet<>();
        } else if (mergeInformation.contains(mergeInfo)) {
            return false;
        }
        mergeInformation.add(mergeInfo);
        return true;
    }

    /**
     * {@inheritDoc}
     * @param mergeInfo information to be stitched.
     */
    @Override
    public void addAllMergeInformation(@Nonnull final Collection<StitchingMergeInformation> mergeInfo) {
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

    @Override
    public boolean removeIfUnstitched() {
        return removeIfUnstitched;
    }

    @Override
    public boolean isStale() {
        return isStale;
    }

    private static <K,V> IdentityHashMap<K, V> miniIdentityHashMap() {
        return new IdentityHashMap<>(MINI_MAP_CAPACITY);
    }

    private static <E> Set<E> smallIdentityHashSet() {
        return Collections.newSetFromMap(new IdentityHashMap<>(SMALL_MAP_CAPACITY));
    }
}
