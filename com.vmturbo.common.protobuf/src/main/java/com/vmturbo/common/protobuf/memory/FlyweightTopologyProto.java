package com.vmturbo.common.protobuf.memory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.AbstractMessage;

import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.HotResizeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.Thresholds;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.TypeCase;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A helper for deduplicating TopologyEntityDTO protobuf messages using an
 * implementation of the {@see <a href="https://en.wikipedia.org/wiki/Flyweight_pattern">Flyweight Pattern</a>}.
 * in order to save memory.
 * <p/>
 * When protobufs are deserialized, duplicate strings are automatically deduplicationd through
 * String interning (except when using Gson to deserialize) but other duplicate messages are
 * not. For example, in the BofA topology, there are 870,996 AnalysisSettings messages, but all
 * but 29 of them are duplicate. If we share duplicate instances instead of using a different
 * instance, this saves a significant chunk of memory.
 * <p/>
 * With the flyweight pattern, there is a tradeoff between time and space. The process of
 * checking whether an object is a duplicate and finding the original flyweight instance
 * (here we use a hashmap) is expensive when performed at scale. Instead of attempting to
 * deduplication every protobuf message (which was tried and abandoned because it took
 * too long), here we use the structure of the topology to only attempt to de-duplciate messages
 * we expect have a high likelihood of being duplicate. This saves a large amount of time while
 * retaining a large chunk of the benefit. For example, on the Bank of America topology from
 * late 2020, deduplicating the whole topology saves about 40% of the memory used by the topology
 * while taking ~70-90 seconds, while deduplicating using the rules encoded in this class saves
 * about 20% of the memory used by the topology while taking about 5 seconds.
 */
public class FlyweightTopologyProto {
    /**
     * The map we use to detect duplicates and identify the flyweight object for de-duplication.
     */
    private final Map<AbstractMessage, AbstractMessage> flyweightCache = new HashMap<>();
    private long dedupNanos = 0;
    private final FlyweightStatistics stats = new FlyweightStatistics();

    /**
     * These entity types tend NOT to share too many duplicated data structures.
     * Because testing for the possibility of de-duplication takes time, when an entity
     * is unlikely to save much memory by trying to deduplicate it, we simply skip
     * the de-duplication attempt and just pay any extra memory cost in order to save
     * processing time.
     */
    public static final Set<Integer> EXCLUDED_ENTITY_TYPES = ImmutableSet.of(
        EntityType.VIRTUAL_DATACENTER_VALUE,    // Only 4.01% saved in BofA
        EntityType.NAMESPACE_VALUE,             // Only 4.34% saved in BofA
        EntityType.SERVICE_VALUE,               // Only 7.14% saved in BofA
        EntityType.CONTAINER_SPEC_VALUE,        // Only 12.32% saved in BofA
        EntityType.BUSINESS_TRANSACTION_VALUE,  // Only 14.78% saved in BofA
        EntityType.APPLICATION_COMPONENT_VALUE  // Only 18.84% saved in BofA
    );

    /**
     * Profiling showed that commodities bought from certain types of providers
     * were very unlikely to be worth de-duplication.
     */
    public static final Set<Integer> EXCLUDED_PROVIDER_TYPES = ImmutableSet.of(
        EntityType.VIRTUAL_DATACENTER_VALUE,    // Only 0% saved in BofA
        EntityType.SERVICE_VALUE,               // Only 0% saved in BofA
        EntityType.BUSINESS_TRANSACTION_VALUE   // Only 0% saved in BofA
    );

    /**
     * Profiling showed that certain types of commodities bought
     * are unlikely to be worth de-duplication.
     */
    public static final Set<Integer> EXCLUDED_COMMODITIES_BOUGHT = ImmutableSet.of(
        CommodityType.CPU_VALUE,                // Only 28.36% in test toppology
        CommodityType.CPU_ALLOCATION_VALUE,     // Only 27.63% in test topology
        CommodityType.Q1_VCPU_VALUE,            // Only 27.51% in test topology
        CommodityType.Q2_VCPU_VALUE,            // Only 27.05% in test topology
        CommodityType.Q4_VCPU_VALUE,            // Only 25.83% in test topology
        CommodityType.Q8_VCPU_VALUE,            // Only 25.26% in test topology
        CommodityType.Q16_VCPU_VALUE,           // Only 26.03% in test topology
        CommodityType.Q32_VCPU_VALUE,           // Only 23.08% in test topology
        CommodityType.Q64_VCPU_VALUE,           // Only 0.00% in test topology
        CommodityType.MEM_VALUE,                // Only 24.17% in test topology
        CommodityType.MEM_ALLOCATION_VALUE,     // Only 24.21% in test topology
        CommodityType.VMEM_VALUE,               // Only 17.74% in test topology
        CommodityType.VCPU_VALUE,               // Only 21.97% in test topology
        CommodityType.VSTORAGE_VALUE,           // Only 14.03% in test topology
        CommodityType.DISK_ARRAY_ACCESS_VALUE,  // Only 0.00% in test topology
        CommodityType.APPLICATION_VALUE         // Only 2.17% in test topology
    );

    /**
     * Profiling showed that certain types of commodities sold
     * are unlikely to be worth de-duplication.
     */
    public static final Set<Integer> WHOLLY_EXCLUDED_COMMODITIES_SOLD = ImmutableSet.of(
        CommodityType.MEM_VALUE,                // Only 23.32% in test topology
        CommodityType.MEM_PROVISIONED_VALUE,    // Only 15.78% in test topology
        CommodityType.MEM_ALLOCATION_VALUE,     // Only 13.94% in test topology
        CommodityType.VMEM_REQUEST_VALUE,       // Only 12.82% in test topology
        CommodityType.CPU_VALUE,                // Only 17.31% in test topology
        CommodityType.CPU_ALLOCATION_VALUE,     // Only 21.03% in test topology
        CommodityType.IO_THROUGHPUT_VALUE,      // Only 17.32% in test topology
        CommodityType.NET_THROUGHPUT_VALUE,     // Only 12.78% in test topology
        CommodityType.VSTORAGE_VALUE,           // Only 16.72% in test topology
        CommodityType.EXTENT_VALUE,             // Only 22.68% in test topology
        CommodityType.DISK_ARRAY_ACCESS_VALUE,  // Only 9.30% in test topology
        CommodityType.APPLICATION_VALUE,        // Only 4.59% in test topology
        CommodityType.VMPM_ACCESS_VALUE         // Only 2.38% in test topology
    );

    /**
     * Profiling showed that certain types of commodities sold have only a subset of fields
     * that are worth attempting to deduplicate, but the entire commodity is not likely to
     * be worth de-duplication.
     */
    public static final Set<Integer> PARTIALLY_EXCLUDED_COMMODITIES_SOLD = ImmutableSet.of(
        CommodityType.VMEM_VALUE,
        CommodityType.VCPU_VALUE
    );

    /**
     * Attempt to deduplicate portions of the entity. The entity returned is guaranteed to be equal
     * to the entity passed in as a parameter (equal by the #equals method, but not necessarily
     * the same (reference-equal) using the == operator). At scale this can save a fairly significant amount
     * of memory, but at the cost of taking some additional time.
     *
     * @param entity The entity whose fields should be attempted to be deduplicated.
     * @return An entity guaranteed to be equal by object equality ({@link Object#equals(Object)} to
     *         the original entity, but not necessarily reference equal (== operator).
     */
    public TopologyEntityDTO tryDeduplicate(@Nonnull TopologyEntityDTO entity) {
        final long startTime = System.nanoTime();
        try {
            stats.totalEntities++;
            if (EXCLUDED_ENTITY_TYPES.contains(entity.getEntityType())) {
                // Don't try to deduplication entities of these types because we don't get
                // much return on the effort.
                stats.skipped++;
                return entity;
            }

            final List<SoldReplacement> soldReplacements = gatherCommoditiesSold(entity);
            final List<BoughtReplacement> boughtReplacements = gatherCommoditiesBought(entity);
            final List<ConnectionReplacement> connectionReplacements = gatherConnections(entity);
            final AnalysisSettings analysisSettingsFlyweight =
                fieldFlyweight(entity::hasAnalysisSettings, entity::getAnalysisSettings);
            final Tags tagsFlyweight = fieldFlyweight(entity::hasTags, entity::getTags);
            final TypeSpecificInfo typeSpecificInfoFlyweight = gatherTypeSpecificInfo(entity);

            return dedupFields(entity, soldReplacements, boughtReplacements,
                connectionReplacements, analysisSettingsFlyweight, tagsFlyweight, typeSpecificInfoFlyweight);
        } finally {
            dedupNanos += (System.nanoTime() - startTime);
        }
    }

    /**
     * Get the amount of time spent deduplicating using this instance.
     *
     * @return the amount of time spent deduplicating using this instance.
     */
    public Duration dedupDuration() {
        return Duration.ofNanos(dedupNanos);
    }

    @Override
    public String toString() {
        return "Took " + dedupDuration() + " " + stats;
    }

    /**
     * Identify if the type-specific information on an entity is duplicated and can be replaced
     * with an equal flyweight object. Returns null if no flyweight is found.
     *
     * @param entity The entity whose type-specific info should be checked.
     * @return The flyweight object, or null if no flyweight is found.
     */
    @Nullable
    private TypeSpecificInfo gatherTypeSpecificInfo(@Nonnull TopologyEntityDTO entity) {
        if (entity.hasTypeSpecificInfo()) {
            final TypeSpecificInfo typeSpecificInfo = entity.getTypeSpecificInfo();
            final TypeCase typeCase = typeSpecificInfo.getTypeCase();
            switch (typeCase) {
                // Only dedup specific type-specific info. Other types of type-specific info are
                // unlikely to be duplicated.
                // For example in a test topology, VirtualMachineInfo only has a 7.5% chance of being
                // duplicated. Some fields such as OS and DriverInfo on VirtualMachineInfo are highly
                // likely to be duplicated, but the cost-tradeoff is not worth it.
                case APPLICATION:           // 99.65% duplication rate
                case DISK_ARRAY:            // 98.73% duplication rate
                case PHYSICAL_MACHINE:      // 95.60% duplication rate
                case STORAGE_CONTROLLER:    // 90.84% duplication rate
                case WORKLOAD_CONTROLLER:   // 93.75% duplication rate
                    return fieldFlyweight(() -> true, () -> typeSpecificInfo);
                default:
                    return null;
            }
        }

        return null;
    }

    /**
     * Attempt to dedup duplicate fields on an entity message. This method will replace duplicate fields on
     * an entity with equal flyweights. The semantics of the object will be equal afterward but if any
     * fields are successfully deduplicated it will use less memory.
     *
     * @param entity The entity whose sub-messages should be de-duplicated.
     * @param soldReplacements Any sold commodities that should be replaced.
     * @param boughtReplacements Any bought commodities that should be replaced.
     * @param connectionReplacements Any connected entity information that should be replaced.
     * @param analysisSettingsFlyweight The analysis settings to replace. Null if no replacement.
     * @param tagsFlyweight The tags to replace. Null if no replacement.
     * @param typeSpecificInfoFlyweight The type specific info to replace. Null if no replacement.
     * @return An equal entity possibly consuming less memory than the one input.
     */
    @Nonnull
    private TopologyEntityDTO dedupFields(@Nonnull final TopologyEntityDTO entity,
                                          @Nonnull final List<SoldReplacement> soldReplacements,
                                          @Nonnull final List<BoughtReplacement> boughtReplacements,
                                          @Nonnull final List<ConnectionReplacement> connectionReplacements,
                                          @Nullable final AnalysisSettings analysisSettingsFlyweight,
                                          @Nullable final Tags tagsFlyweight,
                                          @Nullable final TypeSpecificInfo typeSpecificInfoFlyweight) {
        if (soldReplacements.isEmpty()
            && boughtReplacements.isEmpty()
            && connectionReplacements.isEmpty()
            && analysisSettingsFlyweight == null
            && tagsFlyweight == null
            && typeSpecificInfoFlyweight == null) {

            stats.unmodified++;
            return entity;
        }

        final TopologyEntityDTO.Builder entityBuilder = entity.toBuilder();
        dedupCommoditiesSold(soldReplacements, entityBuilder);
        dedupCommoditiesBought(boughtReplacements, entityBuilder);

        for (ConnectionReplacement connectionReplacement : connectionReplacements) {
            entityBuilder.setConnectedEntityList(connectionReplacement.connectionIndex,
                connectionReplacement.flyweight);
        }
        if (analysisSettingsFlyweight != null) {
            entityBuilder.setAnalysisSettings(analysisSettingsFlyweight);
        }
        if (tagsFlyweight != null) {
            entityBuilder.setTags(tagsFlyweight);
        }
        if (typeSpecificInfoFlyweight != null) {
            entityBuilder.setTypeSpecificInfo(typeSpecificInfoFlyweight);
        }

        stats.partiallyDeduplicated++;
        return entityBuilder.build();
    }

    /**
     * Deduplicate commodities bought by replacing duplicate commodity bought messages or sub-messages with
     * equal flyweights.
     *
     * @param boughtReplacements The replacement commodities bought to use for de-duplication.
     * @param entityBuilder The builder for the entity whose commodities bought should be deduplicated.
     */
    private void dedupCommoditiesBought(@Nonnull List<BoughtReplacement> boughtReplacements, Builder entityBuilder) {
        for (BoughtReplacement boughtReplacement : boughtReplacements) {
            final CommoditiesBoughtFromProvider.Builder providerBuilder =
                entityBuilder.getCommoditiesBoughtFromProvidersBuilder(boughtReplacement.providerIndex);
            if (boughtReplacement.getBoughtFlyweight() != null) {
                // Replace the entire commodity bought
                providerBuilder.setCommodityBought(boughtReplacement.boughtIndex, boughtReplacement.getBoughtFlyweight());
            } else {
                // Replace fields on the commodity bought
                final CommodityBoughtDTO.Builder boughtBuilder = boughtReplacement.original.toBuilder();
                if (boughtReplacement.getCommTypeFlyweight() != null) {
                    boughtBuilder.setCommodityType(boughtReplacement.getCommTypeFlyweight());
                }
                if (boughtReplacement.getHistUsedFlyweight() != null) {
                    boughtBuilder.setHistoricalUsed(boughtReplacement.getHistUsedFlyweight());
                }
                if (boughtReplacement.getHistPeakFlyweight() != null) {
                    boughtBuilder.setHistoricalPeak(boughtReplacement.getHistPeakFlyweight());
                }
                providerBuilder.setCommodityBought(boughtReplacement.boughtIndex, boughtBuilder.build());
            }
        }
    }

    /**
     * Deduplicate commodities sold by replacing duplicate commodity sold messages or sub-messages with
     * equal flyweights.
     *
     * @param soldReplacements The replacement commodities sold to use for de-duplication.
     * @param entityBuilder The builder for the entity whose commodities sold should be deduplicated.
     */
    private void dedupCommoditiesSold(@Nonnull final List<SoldReplacement> soldReplacements,
                                      @Nonnull final Builder entityBuilder) {
        for (SoldReplacement soldReplacement : soldReplacements) {
            if (soldReplacement.getSoldFlyweight() != null) {
                // Replace the entire commodity sold
                entityBuilder.setCommoditySoldList(soldReplacement.soldIndex, soldReplacement.getSoldFlyweight());
            } else {
                // Replace fields on the commodity sold
                final CommoditySoldDTO.Builder soldBuilder = soldReplacement.original.toBuilder();
                if (soldReplacement.getCommTypeFlyweight() != null) {
                    soldBuilder.setCommodityType(soldReplacement.getCommTypeFlyweight());
                }
                if (soldReplacement.getHistUsedFlyweight() != null) {
                    soldBuilder.setHistoricalUsed(soldReplacement.getHistUsedFlyweight());
                }
                if (soldReplacement.getHistPeakFlyweight() != null) {
                    soldBuilder.setHistoricalPeak(soldReplacement.getHistPeakFlyweight());
                }
                if (soldReplacement.getThresholdsFlyweight() != null) {
                    soldBuilder.setThresholds(soldReplacement.getThresholdsFlyweight());
                }
                if (soldReplacement.getHotResizeFlyweight() != null) {
                    soldBuilder.setHotResizeInfo(soldReplacement.getHotResizeFlyweight());
                }
                entityBuilder.setCommoditySoldList(soldReplacement.soldIndex, soldBuilder.build());
            }
        }
    }

    /**
     * Identify if any connected entity information is duplicated and can be replaced
     * with an equal flyweight object. Returns an empty list if there are no replacements.
     *
     * @param entity The entity whose connected-entity info should be checked.
     * @return The flyweight connections, or null if no flyweight is found.
     */
    @Nonnull
    private List<ConnectionReplacement> gatherConnections(
        @Nonnull final TopologyEntityDTO entity) {
        if (entity.getConnectedEntityListCount() == 0) {
            return Collections.emptyList();
        }

        final List<ConnectionReplacement> connectionReplacements = new ArrayList<>();
        final List<ConnectedEntity> connectedEntities = entity.getConnectedEntityListList();
        final int size = connectedEntities.size();
        for (int i = 0; i < size; i++) {
            final ConnectedEntity connection = connectedEntities.get(i);
            final AbstractMessage flyweight = flyweightCache.computeIfAbsent(connection, k -> connection);
            if (flyweight != connection) {
                connectionReplacements.add(new ConnectionReplacement(i, (ConnectedEntity)flyweight));
            }
        }

        return connectionReplacements;
    }

    /**
     * Identify if any commodity bought information is duplicated and can be replaced
     * with equal flyweight messages. Returns an empty list if no flyweights are found.
     *
     * @param entity The entity whose commodity bought info should be checked.
     * @return A list of the flyweights, or an empty list if no flyweights are found.
     */
    @Nonnull
    private List<BoughtReplacement> gatherCommoditiesBought(@Nonnull TopologyEntityDTO entity) {
        final List<BoughtReplacement> boughtReplacements = new ArrayList<>();
        final List<CommoditiesBoughtFromProvider> providerList = entity.getCommoditiesBoughtFromProvidersList();
        final int providersSize = providerList.size();
        for (int i = 0; i < providersSize; i++) {
            final CommoditiesBoughtFromProvider fromProvider = providerList.get(i);
            if (!EXCLUDED_PROVIDER_TYPES.contains(fromProvider.getProviderEntityType())) {
                final List<CommodityBoughtDTO> boughtList = fromProvider.getCommodityBoughtList();
                final int boughtSize = boughtList.size();
                for (int j = 0; j < boughtSize; j++) {
                    final CommodityBoughtDTO bought = boughtList.get(j);
                    final BoughtReplacement replacement = new BoughtReplacement(bought, i, j);
                    final TopologyDTO.CommodityType type = bought.getCommodityType();
                    if (!EXCLUDED_COMMODITIES_BOUGHT.contains(type.getType())) {
                        inspectCommodityBought(boughtReplacements, bought, replacement, type);
                    }
                }
            }
        }
        return boughtReplacements;
    }

    /**
     * Inspect a commodity bought and its sub-messages for any duplication that can be deduplicated.
     *
     * @param boughtReplacements All bought commodities that can be replaced.
     * @param bought The commodity bought to inspect.
     * @param replacement Replacement information for the commodity bought being inspected.
     * @param type The type of the commodity bought.
     */
    private void inspectCommodityBought(@Nonnull final List<BoughtReplacement> boughtReplacements,
                                        @Nonnull final CommodityBoughtDTO bought,
                                        @Nonnull final BoughtReplacement replacement,
                                        @Nonnull final TopologyDTO.CommodityType type) {
        final AbstractMessage boughtFlyweight = flyweightCache.computeIfAbsent(bought, k -> bought);
        if (boughtFlyweight != bought) {
            // If this message is equal to another in the flyweight cache, but is not
            // reference-equal, then we've identified a duplicate and a candidate for deduplication.
            replacement.setBoughtFlyweight((CommodityBoughtDTO)boughtFlyweight);
        } else {
            // If we aren't able to replace the whole commodity, we may be able to replace parts of it.
            final AbstractMessage commTypeFlyweight = flyweightCache.computeIfAbsent(type, k -> type);
            if (commTypeFlyweight != type) {
                replacement.setCommTypeFlyweight((TopologyDTO.CommodityType)commTypeFlyweight);
            }

            if (bought.hasHistoricalUsed()) {
                final HistoricalValues histUsed = bought.getHistoricalUsed();
                final AbstractMessage flyweight = flyweightCache.computeIfAbsent(histUsed, k -> histUsed);
                if (flyweight != histUsed) {
                    replacement.setHistUsedFlyweight((HistoricalValues)flyweight);
                }
            }
            if (bought.hasHistoricalPeak()) {
                final HistoricalValues histPeak = bought.getHistoricalPeak();
                final AbstractMessage flyweight = flyweightCache.computeIfAbsent(histPeak, k -> histPeak);
                if (flyweight != histPeak) {
                    replacement.setHistPeakFlyweight((HistoricalValues)flyweight);
                }
            }
        }
        if (replacement.hasAnyReplacements()) {
            boughtReplacements.add(replacement);
        }
    }

    /**
     * Get the flyweight for a field on an entity.
     *
     * @param hazzer A small predicate function to check whether a field is present.
     *               We only attempt to collect a flyweight if the field is present.
     * @param getter A getter for the field to attempt to deduplicate.
     * @param <T> The type of the field.
     * @return The flyweight for the field if it is duplicated, or null if no flyweight is found.
     */
    @SuppressWarnings("unchecked")
    @Nullable
    private <T extends AbstractMessage> T fieldFlyweight(@Nonnull final BooleanSupplier hazzer,
                                                         @Nonnull final Supplier<T> getter) {
        if (hazzer.getAsBoolean()) {
            final T field = getter.get();
            final AbstractMessage flyweight = flyweightCache.computeIfAbsent(field, k -> field);
            return flyweight == field ? null : (T)flyweight;
        }
        return null;
    }

    /**
     * Identify if any commodity sold information is duplicated and can be replaced
     * with equal flyweight messages. Returns an empty list if no flyweights are found.
     *
     * @param entity The entity whose commodity sold info should be checked.
     * @return A list of the flyweights, or an empty list if no flyweights are found.
     */
    @Nonnull
    private List<SoldReplacement> gatherCommoditiesSold(@Nonnull final TopologyEntityDTO entity) {
        final List<CommoditySoldDTO> commoditySoldList = entity.getCommoditySoldListList();
        final List<SoldReplacement> soldReplacements = new ArrayList<>();

        final int size = commoditySoldList.size();
        for (int i = 0; i < size; i++) {
            final CommoditySoldDTO sold = commoditySoldList.get(i);
            final SoldReplacement replacement = new SoldReplacement(sold, i);
            final TopologyDTO.CommodityType type = sold.getCommodityType();
            if (!WHOLLY_EXCLUDED_COMMODITIES_SOLD.contains(type.getType())) {
                if (!PARTIALLY_EXCLUDED_COMMODITIES_SOLD.contains(type.getType())) {
                    final AbstractMessage flyweight = flyweightCache.computeIfAbsent(sold, k -> sold);
                    if (flyweight != sold) {
                        // If this message is equal to another in the flyweight cache, but is not
                        // reference-equal, then we've identified a duplicate and a candidate for deduplication.
                        replacement.setSoldFlyweight((CommoditySoldDTO)flyweight);
                    }
                }

                // If we aren't able to replace the whole commodity, we may be able to replace parts of it.
                if (replacement.getSoldFlyweight() == null) {
                    inspectCommoditySoldSubmessages(sold, replacement, type);
                }

                // If we are able to deduplicate any fields on the commodity, add it for replacement.
                if (replacement.hasAnyReplacements()) {
                    soldReplacements.add(replacement);
                }
            }
        }

        return soldReplacements;
    }

    /**
     * Inspect the sub-messages within a commodity sold for duplication so that we can save memory.
     *
     * @param sold The commodity sold to inspect.
     * @param replacement Replacement information for the commodity sold.
     * @param type The commodity type for the commodity sold.
     */
    private void inspectCommoditySoldSubmessages(@Nonnull final CommoditySoldDTO sold,
                                                 @Nonnull final SoldReplacement replacement,
                                                 @Nonnull final TopologyDTO.CommodityType type) {
        final AbstractMessage commTypeFlyweight = flyweightCache.computeIfAbsent(type, k -> type);
        if (commTypeFlyweight != type) {
            replacement.setCommTypeFlyweight((TopologyDTO.CommodityType)commTypeFlyweight);
        }

        if (sold.hasHistoricalUsed()) {
            final HistoricalValues histUsed = sold.getHistoricalUsed();
            final AbstractMessage flyweight = flyweightCache.computeIfAbsent(histUsed, k -> histUsed);
            if (flyweight != histUsed) {
                replacement.setHistUsedFlyweight((HistoricalValues)flyweight);
            }
        }
        if (sold.hasHistoricalPeak()) {
            final HistoricalValues histPeak = sold.getHistoricalPeak();
            final AbstractMessage flyweight = flyweightCache.computeIfAbsent(histPeak, k -> histPeak);
            if (flyweight != histPeak) {
                replacement.setHistPeakFlyweight((HistoricalValues)flyweight);
            }
        }
        if (sold.hasThresholds()) {
            final Thresholds thresholds = sold.getThresholds();
            final AbstractMessage flyweight = flyweightCache.computeIfAbsent(thresholds, k -> thresholds);
            if (flyweight != thresholds) {
                replacement.setThresholdsFlyweight((Thresholds)flyweight);
            }
        }
        if (sold.hasHotResizeInfo()) {
            final HotResizeInfo hotResize = sold.getHotResizeInfo();
            final AbstractMessage flyweight = flyweightCache.computeIfAbsent(hotResize, k -> hotResize);
            if (flyweight != hotResize) {
                replacement.setHotResizeFlyweight((HotResizeInfo)flyweight);
            }
        }
    }

    /**
     * Simple statistics about entities and how they were dealt with.
     */
    private static class FlyweightStatistics {
        private long totalEntities;
        private long skipped;
        private long unmodified;
        private long partiallyDeduplicated;

        @Override
        public String toString() {
            return String.format("[%,d total; %,d skipped, %,d not deduplicated, %,d partially deduplicated (%s)]",
                totalEntities, skipped, unmodified, partiallyDeduplicated,
                String.format("%.2f", Duplicates.percentDup(partiallyDeduplicated, totalEntities)) + "%");
        }
    }

    /**
     * A replacement for a connection. Contains the flyweight object for use in replacing an equal
     * connection on another entity.
     */
    private static class ConnectionReplacement {
        private final int connectionIndex;
        private final ConnectedEntity flyweight;

        private ConnectionReplacement(final int connectionIndex,
                                     @Nonnull ConnectedEntity flyweight) {
            this.connectionIndex = connectionIndex;
            this.flyweight = Objects.requireNonNull(flyweight);
        }
    }

    /**
     * A helper class containing fields for use in replacing a commodity sold with equal
     * flyweight objects.
     */
    private static class SoldReplacement {
        private final CommoditySoldDTO original;
        private final int soldIndex;

        private CommoditySoldDTO soldFlyweight;
        private HistoricalValues histUsedFlyweight;
        private HistoricalValues histPeakFlyweight;
        private TopologyDTO.CommodityType commTypeFlyweight;
        private Thresholds thresholdsFlyweight;
        private HotResizeInfo hotResizeFlyweight;

        private SoldReplacement(@Nonnull final CommoditySoldDTO sold,
                                final int soldIndex) {
            this.original = Objects.requireNonNull(sold);
            this.soldIndex = soldIndex;
        }

        private CommoditySoldDTO getSoldFlyweight() {
            return soldFlyweight;
        }

        private void setSoldFlyweight(@Nonnull final CommoditySoldDTO soldFlyweight) {
            this.soldFlyweight = soldFlyweight;
        }

        private HistoricalValues getHistUsedFlyweight() {
            return histUsedFlyweight;
        }

        private void setHistUsedFlyweight(@Nonnull final HistoricalValues histUsedFlyweight) {
            this.histUsedFlyweight = histUsedFlyweight;
        }

        private HistoricalValues getHistPeakFlyweight() {
            return histPeakFlyweight;
        }

        private void setHistPeakFlyweight(@Nonnull final HistoricalValues histPeakFlyweight) {
            this.histPeakFlyweight = histPeakFlyweight;
        }

        private TopologyDTO.CommodityType getCommTypeFlyweight() {
            return commTypeFlyweight;
        }

        private void setCommTypeFlyweight(@Nonnull final TopologyDTO.CommodityType commTypeFlyweight) {
            this.commTypeFlyweight = commTypeFlyweight;
        }

        private Thresholds getThresholdsFlyweight() {
            return thresholdsFlyweight;
        }

        private void setThresholdsFlyweight(@Nonnull final Thresholds thresholdsFlyweight) {
            this.thresholdsFlyweight = thresholdsFlyweight;
        }

        private void setHotResizeFlyweight(@Nonnull final HotResizeInfo hotResizeFlyweight) {
            this.hotResizeFlyweight = hotResizeFlyweight;
        }

        private HotResizeInfo getHotResizeFlyweight() {
            return hotResizeFlyweight;
        }

        private boolean hasAnyReplacements() {
            return soldFlyweight != null
                || histUsedFlyweight != null
                || histPeakFlyweight != null
                || commTypeFlyweight != null
                || thresholdsFlyweight != null
                || hotResizeFlyweight != null;
        }
    }

    /**
     * A helper class for replacing duplicate commodity bought messages with equal flyweights.
     */
    private static class BoughtReplacement {
        private final CommodityBoughtDTO original;
        private final int providerIndex;
        private final int boughtIndex;

        private CommodityBoughtDTO boughtFlyweight;
        private HistoricalValues histUsedFlyweight;
        private HistoricalValues histPeakFlyweight;
        private TopologyDTO.CommodityType commTypeFlyweight;

        private BoughtReplacement(@Nonnull final CommodityBoughtDTO bought,
                                  final int providerIndex, final int boughtIndex) {
            this.original = Objects.requireNonNull(bought);
            this.providerIndex = providerIndex;
            this.boughtIndex = boughtIndex;
        }

        private CommodityBoughtDTO getBoughtFlyweight() {
            return boughtFlyweight;
        }

        private void setBoughtFlyweight(@Nonnull final CommodityBoughtDTO boughtFlyweight) {
            this.boughtFlyweight = boughtFlyweight;
        }

        private HistoricalValues getHistUsedFlyweight() {
            return histUsedFlyweight;
        }

        private void setHistUsedFlyweight(@Nonnull final HistoricalValues histUsedFlyweight) {
            this.histUsedFlyweight = histUsedFlyweight;
        }

        private HistoricalValues getHistPeakFlyweight() {
            return histPeakFlyweight;
        }

        private void setHistPeakFlyweight(@Nonnull final HistoricalValues histPeakFlyweight) {
            this.histPeakFlyweight = histPeakFlyweight;
        }

        private TopologyDTO.CommodityType getCommTypeFlyweight() {
            return commTypeFlyweight;
        }

        private void setCommTypeFlyweight(@Nonnull final TopologyDTO.CommodityType commTypeFlyweight) {
            this.commTypeFlyweight = commTypeFlyweight;
        }

        private boolean hasAnyReplacements() {
            return boughtFlyweight != null
                || histUsedFlyweight != null
                || histPeakFlyweight != null
                || commTypeFlyweight != null;
        }
    }
}
