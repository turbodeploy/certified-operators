package com.vmturbo.stitching.utilities;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;

/**
 * Merge a pair of entity instances down onto a single instance.
 *
 * The specific semantics of how a merge is performed can be tuned, but the general idea is that entities are merged
 * in pairs. One instance in the pair (the "from" entity) will be removed from the topology at the end of the merge
 * and the other instance (the "onto" entity) will be kept at the end of the merge.
 *
 * Any entities buying from the "from" entity before the merge will buy from the "onto" entity after the merge.
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
 *
 * TODO: Support for replacing an entity in discovered groups.
 * TODO: Support for bookkeeping which targets discovered the entity.
 */
public class MergeEntities {
    /**
     * A builder for creating a description of an entity merge change.
     */
    public static class MergeEntitiesStart {
        private final StitchingEntity mergeFromEntity;

        private MergeEntitiesStart(@Nonnull final StitchingEntity mergeFromEntity) {
            this.mergeFromEntity = Objects.requireNonNull(mergeFromEntity);
        }

        public MergeEntitiesDetails onto(@Nonnull final StitchingEntity mergeOntoEntity) {
            return new MergeEntitiesDetails(mergeFromEntity, mergeOntoEntity);
        }
    }

    /**
     * Details describing which entities should be merged along with instructions describing
     * how th emerge should be performed.
     */
    public static class MergeEntitiesDetails {
        private final StitchingEntity mergeFromEntity;
        private final StitchingEntity mergeOntoEntity;
        private MergeCommoditySoldStrategy mergeCommoditySoldStrategy;

        private MergeEntitiesDetails(@Nonnull final StitchingEntity entityToBeReplaced,
                                     @Nonnull final StitchingEntity mergeOntoEntity) {
            this.mergeFromEntity = Objects.requireNonNull(entityToBeReplaced);
            this.mergeOntoEntity = Objects.requireNonNull(mergeOntoEntity);
            this.mergeCommoditySoldStrategy = KEEP_DISTINCT_FAVOR_ONTO;
        }

        public StitchingEntity getMergeFromEntity() {
            return mergeFromEntity;
        }

        public StitchingEntity getMergeOntoEntity() {
            return mergeOntoEntity;
        }

        /**
         * Set the strategy for how commodities sold by the entities should be merged to form
         * a unified set of commodities sold.
         *
         * @param mergeCommoditySoldStrategy The strategy describing how the commodities should be sold.
         * @return A reference to {@link this} for method chaining.
         */
        public MergeEntitiesDetails mergeCommoditiesSoldStrategy(
            @Nonnull final MergeCommoditySoldStrategy mergeCommoditySoldStrategy) {
            this.mergeCommoditySoldStrategy = Objects.requireNonNull(mergeCommoditySoldStrategy);

            return this;
        }

        /**
         * Get the strategy for how commodities sold should be merged.
         *
         * @return the strategy for how commodities sold should be merged.
         */
        public MergeCommoditySoldStrategy getMergeCommoditySoldStrategy() {
            return mergeCommoditySoldStrategy;
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
        return new MergeEntitiesStart(mergeFromEntity);
    }
}
