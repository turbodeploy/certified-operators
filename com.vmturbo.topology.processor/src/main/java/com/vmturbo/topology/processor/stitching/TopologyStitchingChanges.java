package com.vmturbo.topology.processor.stitching;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingResult.StitchingChange;

/**
 * A collection of objects representing the changes that can be made to a topology during stitching.
 */
public class TopologyStitchingChanges {
    private static final Logger logger = LogManager.getLogger();

    private TopologyStitchingChanges() {
        // Never instantiate this class. Only inner classes may be instantiated.s
    }

    /**
     * Represents the removal of an individual {@link StitchingEntity} from the eventual topology.
     */
    @Immutable
    public static class RemoveEntityChange implements StitchingChange {
        private final StitchingEntity entityToRemove;
        private final StitchingContext stitchingContext;

        public RemoveEntityChange(@Nonnull final StitchingContext stitchingContext,
                                  @Nonnull final StitchingEntity entityToRemove) {
            this.entityToRemove = Objects.requireNonNull(entityToRemove);
            this.stitchingContext = Objects.requireNonNull(stitchingContext);
        }

        @Override
        public void applyChange() {
            Preconditions.checkArgument(entityToRemove instanceof TopologyStitchingEntity);
            final TopologyStitchingEntity removed = (TopologyStitchingEntity)entityToRemove;
            stitchingContext.removeEntity(removed);
        }
    }

    /**
     * Represents replacing one entity with another.
     */
    @Immutable
    public static class MergeEntitiesChange implements  StitchingChange {
        private final StitchingContext stitchingContext;
        private final StitchingEntity mergeFromEntity;
        private final StitchingEntity mergeOntoEntity;
        private final CommoditySoldMerger commoditySoldMerger;

        public MergeEntitiesChange(@Nonnull final StitchingContext stitchingContext,
                                   @Nonnull final StitchingEntity mergeFromEntity,
                                   @Nonnull final StitchingEntity mergeOntoEntity,
                                   @Nonnull final CommoditySoldMerger commoditySoldMerger) {
            this.stitchingContext = Objects.requireNonNull(stitchingContext);
            this.mergeFromEntity = Objects.requireNonNull(mergeFromEntity);
            this.mergeOntoEntity = Objects.requireNonNull(mergeOntoEntity);
            this.commoditySoldMerger = Objects.requireNonNull(commoditySoldMerger);
        }

        @Override
        public void applyChange() {
            Preconditions.checkArgument(mergeFromEntity instanceof TopologyStitchingEntity);
            Preconditions.checkArgument(mergeOntoEntity instanceof TopologyStitchingEntity);
            Preconditions.checkArgument(stitchingContext.hasEntity(mergeOntoEntity));
            if (mergeFromEntity == mergeOntoEntity) {
                logger.debug("mergeFromEntity and mergeOntoEntity {} are the same. " +
                    "Merging an entity onto itself is a no-op.", mergeFromEntity);
                return;
            }
            if (!stitchingContext.hasEntity(mergeFromEntity)) {
                logger.debug("mergeFromEntity {} is not in the StitchingContext so not applying. " +
                    "Was it already merged or removed?", mergeFromEntity);
                return;
            }

            final TopologyStitchingEntity from = (TopologyStitchingEntity) mergeFromEntity;
            final TopologyStitchingEntity onto = (TopologyStitchingEntity) mergeOntoEntity;

            // TODO: (DavidBlinn 12/1/2017)
            // TODO: Support for replacing an entity in discovered groups.
            // TODO: Support for bookkeeping which targets discovered the entity.

            // Set up commodities sold on the merged (onto) entity.
            onto.setCommoditiesSold(commoditySoldMerger.mergeCommoditiesSold(
                from.getTopologyCommoditiesSold(),
                onto.getTopologyCommoditiesSold()
            ));

            // Everything that used to buy from replaced should now buy from replacement.
            from.getConsumers().forEach(consumer ->
                buyFromNewProvider(consumer, from, onto));

            stitchingContext.removeEntity(from);
        }

        /**
         * Make the entity toUpdate buy everything that it used to buy from the oldProvider now buy from the
         * newProvider.
         *
         * @param toUpdate The entity to update.
         * @param oldProvider The provider that the entity to update should no longer buy from.
         * @param newProvider The provider that the entity to update should now buy from.
         */
        private void buyFromNewProvider(@Nonnull final StitchingEntity toUpdate,
                                        @Nonnull final TopologyStitchingEntity oldProvider,
                                        @Nonnull final TopologyStitchingEntity newProvider) {
            // All commodities that used to be bought by the old provider should now be bought from the new provider.
            List<CommodityDTO.Builder> commoditiesBought = toUpdate.getCommoditiesBoughtByProvider().remove(oldProvider);
            if (commoditiesBought == null) {
                throw new IllegalStateException("Entity " + toUpdate + " is a consumer of " + oldProvider
                    + " but is not buying any commodities from it.");
            }

            final List<CommodityDTO.Builder> boughtFromProvider = toUpdate.getCommoditiesBoughtByProvider()
                .computeIfAbsent(newProvider, provider -> new ArrayList<>(commoditiesBought.size()));
            // TODO: Consider adding support for a consumerCommoditiesBoughtMerger
            boughtFromProvider.addAll(commoditiesBought);

            // Make the buying entity a consumer of the new provider.
            newProvider.addConsumer(toUpdate);
        }
    }

    /**
     * Represents the update of relationships of an individual {@link StitchingEntity} in
     * the eventual topology based on the changes to the commodities bought.
     *
     * We do NOT currently support destructive changes to commodities sold.
     */
    @Immutable
    public static class UpdateEntityRelationshipsChange implements StitchingChange {
        private final StitchingEntity entityToUpdate;
        private final Consumer<StitchingEntity> updateMethod;

        public UpdateEntityRelationshipsChange(@Nonnull final StitchingEntity entityToUpdate,
                                               @Nonnull final Consumer<StitchingEntity> updateMethod) {
            this.entityToUpdate = entityToUpdate;
            this.updateMethod = updateMethod;
        }

        @Override
        public void applyChange() {
            Preconditions.checkArgument(entityToUpdate instanceof TopologyStitchingEntity);

            // Track providers before and after applying the update.
            final List<StitchingEntity> providersBeforeChangeCopy = entityToUpdate.getProviders().stream()
                .collect(Collectors.toList());
            updateMethod.accept(entityToUpdate);
            final Set<StitchingEntity> providersAfterChange = entityToUpdate.getProviders();

            // All removed providers should no longer relate to the destination through a consumer relationship.
            providersBeforeChangeCopy.stream()
                .filter(provider -> !providersAfterChange.contains(provider))
                .forEach(provider -> ((TopologyStitchingEntity)provider).removeConsumer(entityToUpdate));
            // All added providers should now relate to the destination through a consumer relationship.
            providersAfterChange.stream()
                .filter(provider -> !providersBeforeChangeCopy.contains(provider))
                .forEach(provider -> ((TopologyStitchingEntity)provider).addConsumer(entityToUpdate));
        }
    }

    /**
     * A stitching change that makes no changes to relationships on any entity in the topology.
     * This sort of change may update the builder or the values in some commodity on a single entity.
     */
    public static class UpdateEntityAloneChange implements StitchingChange {
        private final StitchingEntity entityToUpdate;
        private final Consumer<StitchingEntity> updateMethod;

        public UpdateEntityAloneChange(@Nonnull final StitchingEntity entityToUpdate,
                                       @Nonnull final Consumer<StitchingEntity> updateMethod) {
            this.entityToUpdate = Objects.requireNonNull(entityToUpdate);
            this.updateMethod = Objects.requireNonNull(updateMethod);
        }

        @Override
        public void applyChange() {
            updateMethod.accept(entityToUpdate);
        }
    }
}
