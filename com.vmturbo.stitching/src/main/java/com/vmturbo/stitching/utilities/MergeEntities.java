package com.vmturbo.stitching.utilities;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.utilities.EntityFieldMergers.EntityFieldMerger;

/**
 * Merge a pair of entity instances down onto a single instance.
 *
 * The specific semantics of how a merge is performed can be tuned, but the general idea is that entities are merged
 * in pairs. One instance in the pair (the "from" entity) will be removed from the topology at the end of the merge
 * and the other instance (the "onto" entity) will be kept at the end of the merge.
 *
 * Any entities buying from the "from" entity before the merge will buy from the "onto" entity after the merge.
 *
 * Control the merge of individual fields by adding an {@link EntityFieldMerger}.
 * Field mergers are run in the order in which they are added to the list of field mergers.
 * Field mergers are run prior to merging commodities sold.
 *
 * Commodities sold are merged using a {@link com.vmturbo.stitching.utilities.MergeEntities.MergeCommoditySoldStrategy}.
 * By default, the strategy will keep distinct commodities sold (where uniqueness is determined by the combination of
 * a commodity's type and its key), and in the case of an overlap where both the "from" and "onto" are selling
 * the same commodity, the commodity sold by the "onto" entity is kept.
 *
 * Merging FROM an entity ONTO itself results in a no-op.
 * Merging FROM an entity NOT IN the topology ONTO an entity IN the topology results in a no-op.
 * Merging ONTO an entity NOT IN the topology results in an exception.
 *
 * @see StitchingChangesBuilder#queueEntityMerger(MergeEntitiesDetails)
 */
public class MergeEntities {
    private static final Logger logger = LogManager.getLogger();

    /**
     * A builder for creating a description of an entity merge change.
     */
    public static class MergeEntitiesStart {
        private final List<StitchingEntity> mergeFromEntities;

        private MergeEntitiesStart(@Nonnull final List<StitchingEntity> mergeFromEntities) {
            this.mergeFromEntities = Objects.requireNonNull(mergeFromEntities);
        }

        public MergeEntitiesDetails onto(@Nonnull final StitchingEntity mergeOntoEntity) {
            return onto(mergeOntoEntity, MergePropertiesStrategy.KEEP_ONTO, true);
        }

        public MergeEntitiesDetails onto(@Nonnull final StitchingEntity mergeOntoEntity,
                                         @Nonnull final MergeCommoditySoldStrategy mergeCommoditySoldStrategy) {
            return onto(mergeOntoEntity, mergeCommoditySoldStrategy,
                    MergePropertiesStrategy.KEEP_ONTO, true);
        }

        /**
         * Create {@link MergeEntitiesDetails} for given "onto" entity, merge properties
         * strategy and the given selected fields.
         *
         * @param mergeOntoEntity "Onto" entity to merge.
         * @param mergePropertiesStrategy Merge properties strategy.
         * @param mergeCommodities Merge commodities.
         * @return New instance of {@link MergeEntitiesDetails}.
         */
        public MergeEntitiesDetails onto(@Nonnull final StitchingEntity mergeOntoEntity,
                @Nonnull final MergePropertiesStrategy mergePropertiesStrategy,
                final boolean mergeCommodities) {
            return onto(mergeOntoEntity, KEEP_DISTINCT_FAVOR_ONTO, mergePropertiesStrategy, mergeCommodities);
        }

        /**
         * Create {@link MergeEntitiesDetails} for given "onto" entity, merge sold commodities
         * strategy and merge properties strategy.
         *
         * @param mergeOntoEntity "Onto" entity to merge.
         * @param mergeCommoditySoldStrategy Merge sold commodities strategy.
         * @param mergePropertiesStrategy Merge properties strategy.
         * @param mergeCommodities Merge commodities.
         * @return New instance of {@link MergeEntitiesDetails}.
         */
        public MergeEntitiesDetails onto(@Nonnull final StitchingEntity mergeOntoEntity,
                                         @Nonnull final MergeCommoditySoldStrategy mergeCommoditySoldStrategy,
                                         @Nonnull final MergePropertiesStrategy mergePropertiesStrategy,
                                         final boolean mergeCommodities) {
            return new MergeEntitiesDetails(mergeFromEntities, mergeOntoEntity,
                    mergeCommoditySoldStrategy, mergePropertiesStrategy, mergeCommodities);
        }
    }

    /**
     * Details describing which entities should be merged along with instructions describing
     * how the merge should be performed.
     */
    public static class MergeEntitiesDetails {
        private final List<StitchingEntity> mergeFromEntities;
        private final StitchingEntity mergeOntoEntity;
        private final MergeCommoditySoldStrategy mergeCommoditySoldStrategy;
        private final MergePropertiesStrategy mergePropertiesStrategy;
        private final List<EntityFieldMerger<?>> fieldMergers;
        private final boolean mergeCommodities;

        private MergeEntitiesDetails(@Nonnull final List<StitchingEntity> entitiesToBeReplaced,
                                     @Nonnull final StitchingEntity mergeOntoEntity,
                                     @Nonnull final MergeCommoditySoldStrategy mergeCommoditySoldStrategy,
                                     @Nonnull final MergePropertiesStrategy mergePropertiesStrategy,
                                     final boolean mergeCommodities) {
            this.mergeFromEntities = Objects.requireNonNull(entitiesToBeReplaced);
            this.mergeOntoEntity = Objects.requireNonNull(mergeOntoEntity);
            this.mergeCommoditySoldStrategy = mergeCommoditySoldStrategy;
            this.mergePropertiesStrategy = mergePropertiesStrategy;
            this.fieldMergers = new ArrayList<>();
            this.mergeCommodities = mergeCommodities;
        }

        public List<StitchingEntity> getMergeFromEntities() {
            return mergeFromEntities;
        }

        public StitchingEntity getMergeOntoEntity() {
            return mergeOntoEntity;
        }

        /**
         * Get the strategy for how commodities sold by the entities should be merged to form
         * a unified set of commodities sold.
         *
         * @return The strategy describing how the commodities should be merged.
         */
        public MergeCommoditySoldStrategy getMergeCommoditySoldStrategy() {
            return mergeCommoditySoldStrategy;
        }

        /**
         * Get the strategy describing how to merge entity properties.
         *
         * @return The strategy describing how to merge entity properties.
         */
        public MergePropertiesStrategy getMergePropertiesStrategy() {
            return mergePropertiesStrategy;
        }

        /**
         * Add a merger to merge a specific field.
         *
         * @param fieldMerger The field merger to add.
         * @return A reference to {@link this} for method chaining.
         */
        public MergeEntitiesDetails addFieldMerger(@Nonnull final EntityFieldMerger<?> fieldMerger) {
            if (fieldMergers.stream().anyMatch(merger -> merger.getGetter() == fieldMerger.getGetter())) {
                logger.warn("Adding a fieldMerger for merging entities that overrides an existing merger.");
            }

            this.fieldMergers.add(fieldMerger);
            return this;
        }

        /**
         * Add a collection of mergers to merge a specific field.
         *
         * @param fieldMergers The field mergers to add.
         * @return A reference to {@link this} for method chaining.
         */
        public MergeEntitiesDetails addAllFieldMergers(
            @Nonnull final Collection<EntityFieldMerger<?>> fieldMergers) {
            fieldMergers.forEach(this::addFieldMerger);
            return this;
        }

        /**
         * Get the mergers for merging specific fields.
         *
         * @return The field mergers to use to merge specific fields.
         */
        public List<EntityFieldMerger<?>> getFieldMergers() {
            return Collections.unmodifiableList(fieldMergers);
        }

        /**
         * Check whether to merge commodities or not.
         *
         * @return true if commodities should be merged.
         */
        public boolean mergeCommodities() {
            return mergeCommodities;
        }
    }

    /**
     * A strategy that determines how to merge the commodities sold by entities that are themselves being merged.
     *
     * Sold commodity uniqueness is determined by the combination of the commodity type and key.
     *
     * When merging entities, the commodities sold by those entities are examined and dispatched to the strategy
     * to determine which commodities to discard/keep/modify.
     *
     * TODO: Support for controlling the accesses relationship on a commodity sold if necessary.
     */
    public interface MergeCommoditySoldStrategy {
        /**
         * The {@link Origin} describes which entity the commodity came from (ie the "from" entity in the merger
         * or the "onto" entity).
         */
        enum Origin {
            FROM_ENTITY,
            ONTO_ENTITY
        }

        /**
         * Called when one of the entities being merged sells a commodity but the other entity does not.
         *
         * @param commodity The commodity to merge.
         * @param origin Which of the two entities in the merge (the "from" or the "onto") was the source
         *               of this commodity.
         * @return The commodity that the merged entity should sell. Return {@link Optional#empty()} to discard
         *         the commodity. Return an optional containing the commodity to keep it on the entity.
         *         The commodity can be mutated using the builder if any of its properties should
         *         be changed.
         */
        @Nonnull
        Optional<CommodityDTO.Builder> onDistinctCommodity(@Nonnull final CommodityDTO.Builder commodity,
                                                           final Origin origin);

        /**
         * Called when both of the entities being merged sells a commodity.
         *
         * @param fromCommodity The commodity sold by the "from" entity in the merger.
         * @param ontoCommodity The commodity sold by the "onto" entity in the merger.
         * @return The commodity that the merged entity should sell. Return {@link Optional#empty()} to discard
         *         the commodity. Return an optional containing the commodity to keep it on the entity.
         *         The commodity can be mutated using the builder if any of its properties should
         *         be changed.
         */
        @Nonnull
        Optional<CommodityDTO.Builder> onOverlappingCommodity(@Nonnull final CommodityDTO.Builder fromCommodity,
                                                              @Nonnull final CommodityDTO.Builder ontoCommodity);

        /**
         * Check if the commodity of type fromCommodityType (from the fromEntity) should be ignored
         * if there is already an existing commodity of same type on the ontoEntity. It's false by
         * default, which means all commodities defined in stitching metadata will be merged, and
         * there may be multiple commodities of same type but different keys.
         *
         * @param fromCommodityType the commodity from the fromEntity which should be checked
         * @return true if the commodity should be ignored, otherwise false.
         */
        default boolean ignoreIfPresent(@Nonnull final CommodityType fromCommodityType) {
            return false;
        }
    }

    /**
     * An easy default {@link com.vmturbo.stitching.utilities.MergeEntities.MergeCommoditySoldStrategy}.
     * Commodity uniqueness is determined by the combination of the commodity type and key.
     *
     * In the case of a distinct commodity (sold by only one of the entities, commonly seen in cases of, for example,
     * storage access commodities where the commodity key will be unique to a discovered instance of an entity),
     * keep that commodity as unchanged.
     *
     * In the case of an overlapping commodity (sold by both the "from" and "onto" entities in the merge),
     * keep the commodity from the "onto" entity in favor of the one from the "from" entity.
     */
    public static MergeCommoditySoldStrategy KEEP_DISTINCT_FAVOR_ONTO = new MergeCommoditySoldStrategy() {
        @Nonnull
        @Override
        public Optional<Builder> onDistinctCommodity(@Nonnull final CommodityDTO.Builder commodity,
                                                     final Origin origin) {
            return Optional.of(commodity);
        }

        @Nonnull
        @Override
        public Optional<Builder> onOverlappingCommodity(@Nonnull final CommodityDTO.Builder fromCommodity,
                                                        @Nonnull final CommodityDTO.Builder ontoCommodity) {
            return Optional.of(ontoCommodity);
        }
    };

    /**
     * A strategy to drop all the sold commodities on the 'from' entity, and only keep the sold
     * commodities on the 'to' entity.
     */
    public static final MergeCommoditySoldStrategy DROP_ALL_FROM_COMMODITIES_STRATEGY = new MergeCommoditySoldStrategy() {
        @Nonnull
        @Override
        public Optional<Builder> onDistinctCommodity(@Nonnull final CommodityDTO.Builder commodity,
                                                     @Nonnull final Origin origin) {
            return origin == Origin.ONTO_ENTITY ? Optional.of(commodity) : Optional.empty();
        }

        @Nonnull
        @Override
        public Optional<Builder> onOverlappingCommodity(@Nonnull final CommodityDTO.Builder fromCommodity,
                                                        @Nonnull final CommodityDTO.Builder ontoCommodity) {
            return Optional.of(ontoCommodity);
        }
    };

    /**
     * Create a change description for merging two instances of an entity onto a single instance.
     *
     * @param mergeFromEntity The entity that should be merged "from" onto the "onto" entity.
     *                        This instance of the entity will be removed from the topology at the
     *                        end of the merge operation while the "onto" entity that has had the result
     *                        of the merge placed onto it will be kept.
     * @return A {@link com.vmturbo.stitching.utilities.MergeEntities.MergeEntitiesStart} object for building
     *         the description of the merge.
     */
    public static MergeEntitiesStart mergeEntity(@Nonnull final StitchingEntity mergeFromEntity) {
        return new MergeEntitiesStart(Collections.singletonList(mergeFromEntity));
    }

    /**
     * Create a change description for merging a list of instances of an entity onto a single instance.
     *
     * @param mergeFromEntities The entities that should be merged "from" onto the "onto" entity.
     *                          These instances of the entity will be removed from the topology at the
     *                          end of the merge operation while the "onto" entity that has had the result
     *                          of the merge placed onto it will be kept.
     * @return A {@link com.vmturbo.stitching.utilities.MergeEntities.MergeEntitiesStart} object for building
     *         the description of the merge.
     */
    public static MergeEntitiesStart mergeEntities(@Nonnull final List<StitchingEntity> mergeFromEntities) {
        return new MergeEntitiesStart(mergeFromEntities);
    }
}
