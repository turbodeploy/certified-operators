package com.vmturbo.topology.processor.stitching;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.stitching.EntityToAdd;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingMergeInformation;
import com.vmturbo.stitching.TopologicalChangelog.TopologicalChange;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.journal.IStitchingJournal.JournalChangeset;
import com.vmturbo.stitching.journal.JournalableEntity;
import com.vmturbo.stitching.utilities.CommoditiesBought;
import com.vmturbo.stitching.utilities.CopyCommodities;
import com.vmturbo.stitching.utilities.EntityFieldMergers.EntityFieldMerger;
import com.vmturbo.stitching.utilities.MergeEntities.MergeEntitiesDetails;

/**
 * A collection of objects representing the changes that can be made to a topology during stitching.
 */
public class TopologyStitchingChanges {
    private static final Logger logger = LogManager.getLogger();

    private TopologyStitchingChanges() {
        // Never instantiate this class. Only inner classes may be instantiated.s
    }

    /**
     * Base implementation for {@link TopologicalChange} subclasses.
     */
    public abstract static class BaseTopologicalChange<ENTITY extends JournalableEntity<ENTITY>>
        implements TopologicalChange<ENTITY> {
        @Override
        public void applyChange(@Nonnull final IStitchingJournal<ENTITY> stitchingJournal) {
            stitchingJournal.recordChangeset(getPreamble(), this::applyChangeInternal);
        }

        protected abstract String getPreamble();

        protected abstract void applyChangeInternal(
            @Nonnull final JournalChangeset<ENTITY> changeset);
    }

    /**
     * Represents the removal of an individual {@link StitchingEntity} from the eventual topology.
     */
    @Immutable
    public static class RemoveEntityChange extends BaseTopologicalChange<StitchingEntity> {
        private final StitchingEntity entityToRemove;
        private final StitchingContext stitchingContext;

        public RemoveEntityChange(@Nonnull final StitchingContext stitchingContext,
                                  @Nonnull final StitchingEntity entityToRemove) {
            this.entityToRemove = Objects.requireNonNull(entityToRemove);
            this.stitchingContext = Objects.requireNonNull(stitchingContext);
        }

        @Override
        protected String getPreamble() {
            return "Removing entity " + entityToRemove.getJournalableSignature();
        }

        @Override
        protected void applyChangeInternal(@Nonnull final JournalChangeset<StitchingEntity> changeset) {
            Preconditions.checkArgument(entityToRemove instanceof TopologyStitchingEntity);
            final TopologyStitchingEntity removed = (TopologyStitchingEntity)entityToRemove;
            changeset.observeRemoval(removed);
            stitchingContext.removeEntity(removed);
        }
    }

    @Immutable
    public static class AddEntitiesChange extends BaseTopologicalChange<StitchingEntity> {

        private final StitchingContext stitchingContext;

        private final List<EntityToAdd> entities;

        public AddEntitiesChange(@Nonnull final StitchingContext stitchingContext,
                                 @Nonnull final List<EntityToAdd> entities) {
            this.stitchingContext = Objects.requireNonNull(stitchingContext);
            this.entities = Objects.requireNonNull(entities);
        }

        @Override
        protected String getPreamble() {
            return "Adding " + entities.size() + " new entities and set up relationships";
        }

        @Override
        protected void applyChangeInternal(@Nonnull final JournalChangeset<StitchingEntity> changeset) {
            // track the changes for consumer, for example: connected relationship
            entities.forEach(entity -> changeset.beforeChange(entity.getConsumer()));
            // add new entities to graph
            List<TopologyStitchingEntity> newEntities = stitchingContext.addEntities(entities);
            // add the new added entities to StitchingJournal
            newEntities.forEach(entity -> changeset.observeAddition(entity));
        }
    }

    /**
     * Represents replacing one entity with another.
     */
    @Immutable
    public static class MergeEntitiesChange extends BaseTopologicalChange<StitchingEntity> {
        private final StitchingContext stitchingContext;
        private final StitchingEntity mergeFromEntity;
        private final StitchingEntity mergeOntoEntity;
        private final CommoditySoldMerger commoditySoldMerger;
        private final List<EntityFieldMerger<?>> fieldMergers;

        public MergeEntitiesChange(@Nonnull final StitchingContext stitchingContext,
                                   @Nonnull final StitchingEntity mergeFromEntity,
                                   @Nonnull final StitchingEntity mergeOntoEntity,
                                   @Nonnull final CommoditySoldMerger commoditySoldMerger,
                                   @Nonnull final List<EntityFieldMerger<?>> fieldMergers) {
            this.stitchingContext = Objects.requireNonNull(stitchingContext);
            this.mergeFromEntity = Objects.requireNonNull(mergeFromEntity);
            this.mergeOntoEntity = Objects.requireNonNull(mergeOntoEntity);
            this.commoditySoldMerger = Objects.requireNonNull(commoditySoldMerger);
            this.fieldMergers = Objects.requireNonNull(fieldMergers);
        }

        public MergeEntitiesChange(@Nonnull final StitchingContext stitchingContext,
                                   @Nonnull final MergeEntitiesDetails mergeDetails) {
            this(stitchingContext,
                mergeDetails.getMergeFromEntity(),
                mergeDetails.getMergeOntoEntity(),
                new CommoditySoldMerger(mergeDetails.getMergeCommoditySoldStrategy()),
                mergeDetails.getFieldMergers());
        }

        @Override
        protected String getPreamble() {
            return "Merging from " + mergeFromEntity.getJournalableSignature() + " onto "
                + mergeOntoEntity.getJournalableSignature();
        }

        @Override
        protected void applyChangeInternal(@Nonnull final JournalChangeset<StitchingEntity> changeset) {
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

            final TopologyStitchingEntity from = (TopologyStitchingEntity)mergeFromEntity;
            final TopologyStitchingEntity onto = (TopologyStitchingEntity)mergeOntoEntity;
            changeset.beforeChange(from);
            changeset.beforeChange(onto);

            // Run all custom field mergers.
            fieldMergers.forEach(merger -> merger.merge(from, onto));

            // Set up commodities sold on the merged (onto) entity.
            onto.setCommoditiesSold(commoditySoldMerger.mergeCommoditiesSold(
                from.getTopologyCommoditiesSold(),
                onto.getTopologyCommoditiesSold()
            ));

            // Everything that used to buy from replaced should now buy from replacement.
            from.getConsumers().forEach(consumer -> {
                changeset.beforeChange(consumer);
                buyFromNewProvider(consumer, from, onto);
            });

            // merge "connectedTo" from "from" entity to "onto" entity
            from.getConnectedToByType().forEach((connectionType, connectedEntities) -> {
                onto.addConnectedTo(connectionType, connectedEntities);
                connectedEntities.stream()
                        .filter(stitchingEntity -> stitchingEntity
                                instanceof TopologyStitchingEntity)
                        .map(TopologyStitchingEntity.class::cast)
                        .forEach(topoStitchingEntity -> {
                            changeset.beforeChange(topoStitchingEntity);
                            topoStitchingEntity.removeConnectedFrom(connectionType, from);
                            topoStitchingEntity.addConnectedFrom(connectionType, onto);
                        });
                    }
            );

            // merge "connectedFrom" from "from" entity to "onto" entity.  In this case, we need to
            // make sure the other end of connectedFrom has connectedTo moved from the "from" entity
            // to the "onto" entity.
            from.getConnectedFromByType().forEach((connectionType, connectedEntities) -> {
                    onto.addConnectedFrom(connectionType, connectedEntities);
                        connectedEntities.stream()
                                .filter(stitchingEntity -> stitchingEntity
                                        instanceof TopologyStitchingEntity)
                                .map(TopologyStitchingEntity.class::cast)
                                .forEach(topoStitchingEntity -> {
                                    changeset.beforeChange(topoStitchingEntity);
                                    topoStitchingEntity.removeConnectedTo(connectionType, from);
                                    topoStitchingEntity.addConnectedTo(connectionType, onto);
                                });
                    }

            );

            trackMergeInformation(from, onto);
            stitchingContext.removeEntity(from);
            changeset.observeRemoval(from);
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
            Optional<List<CommoditiesBought>> commoditiesBought = toUpdate.removeProvider(oldProvider);
            if (!commoditiesBought.isPresent()) {
                throw new IllegalStateException("Entity " + toUpdate + " is a consumer of " + oldProvider
                        + " but is not buying any commodities from it.");
            }

            final List<CommoditiesBought> commoditiesBoughtList =
                toUpdate.getCommodityBoughtListByProvider().computeIfAbsent(newProvider, p ->
                    new ArrayList<>(commoditiesBought.get().size()));
            // merge old commodities bought list from old provider to new provider
            commoditiesBought.get().forEach(fromCommoditiesBought -> {
                final Optional<CommoditiesBought> matchingCommoditiesBought =
                    toUpdate.getMatchingCommoditiesBought(newProvider, fromCommoditiesBought);
                if (matchingCommoditiesBought.isPresent()) {
                    // merge from fromCommoditiesBought to ontoCommoditiesBought
                    final CommoditiesBought ontoCommoditiesBought = matchingCommoditiesBought.get();
                    final List<CommodityDTO.Builder> mergedBoughtCommodities =
                        CopyCommodities.mergeCommoditiesBought(fromCommoditiesBought.getBoughtList(),
                            ontoCommoditiesBought.getBoughtList(), Optional.empty(), false);
                    // remove old commodities bought set
                    commoditiesBoughtList.remove(ontoCommoditiesBought);
                    // add new merged commodities bought set
                    commoditiesBoughtList.add(new CommoditiesBought(mergedBoughtCommodities,
                        ontoCommoditiesBought.getVolumeId()));
                } else {
                    List<CommodityDTO.Builder> newBoughtList = CopyCommodities.matchBoughtToSold(
                            fromCommoditiesBought.getBoughtList(),
                            newProvider.getCommoditiesSold().collect(Collectors.toList())
                    );
                    CommoditiesBought newCommoditiesBought = new CommoditiesBought(newBoughtList);
                    fromCommoditiesBought.getMovable().ifPresent(newCommoditiesBought::setMovable);
                    fromCommoditiesBought.getStartable().ifPresent(newCommoditiesBought::setStartable);
                    fromCommoditiesBought.getScalable().ifPresent(newCommoditiesBought::setScalable);
                    Long volumeId = fromCommoditiesBought.getVolumeId();
                    if (volumeId != null) {
                        newCommoditiesBought.setVolumeId(volumeId);
                    }
                    commoditiesBoughtList.add(newCommoditiesBought);
                }
            });

            // Make the buying entity a consumer of the new provider.
            newProvider.addConsumer(toUpdate);
        }

        /**
         * Track the oids and targetIds of all targets that discovered the "from" entity by
         * adding those targetIds and oids onto the mergeInformation of the "onto" entity.
         *
         * Also updates the last updatedTime on the "onto" entity to be
         * max(from.updateTime, onto.updateTime)
         *
         * @param from The entity whose data will be merged onto the "onto" entity.
         * @param onto The entity to receive data from the "from" entity.
         */
        private void trackMergeInformation(@Nonnull final TopologyStitchingEntity from,
                                           @Nonnull final TopologyStitchingEntity onto) {
            onto.addMergeInformation(new StitchingMergeInformation(from));
            onto.addAllMergeInformation(from.getMergeInformation());
            onto.updateLastUpdatedTime(from.getLastUpdatedTime());
        }
    }

    /**
     * Represents the update of relationships of an individual {@link StitchingEntity} in
     * the eventual topology based on the changes to the commodities bought.
     *
     * We do NOT currently support destructive changes to commodities sold.
     */
    @Immutable
    public static class UpdateEntityRelationshipsChange extends BaseTopologicalChange<StitchingEntity> {
        private final StitchingEntity entityToUpdate;
        private final Consumer<StitchingEntity> updateMethod;

        public UpdateEntityRelationshipsChange(@Nonnull final StitchingEntity entityToUpdate,
                                               @Nonnull final Consumer<StitchingEntity> updateMethod) {
            this.entityToUpdate = entityToUpdate;
            this.updateMethod = updateMethod;
        }

        @Override
        protected String getPreamble() {
            return "Updating entity properties and relationships for " + entityToUpdate.getJournalableSignature();
        }

        @Override
        protected void applyChangeInternal(@Nonnull final JournalChangeset<StitchingEntity> changeset) {
            Preconditions.checkArgument(entityToUpdate instanceof TopologyStitchingEntity);
            changeset.beforeChange(entityToUpdate);

            // Track providers before and after applying the update.
            final List<StitchingEntity> providersBeforeChangeCopy = entityToUpdate.getProviders().stream()
                .collect(Collectors.toList());
            final Map<ConnectionType, Set<StitchingEntity>> connectedToBeforeChangeCopy =
                entityToUpdate.getConnectedToByType().entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getKey(), e -> ImmutableSet.copyOf(e.getValue())));
            updateMethod.accept(entityToUpdate);

            final Set<StitchingEntity> providersAfterChange = entityToUpdate.getProviders();
            final Map<ConnectionType, Set<StitchingEntity>> connectedToAfterChange =
                entityToUpdate.getConnectedToByType();

            // All removed providers should no longer relate to the destination through a consumer relationship.
            providersBeforeChangeCopy.stream()
                .filter(provider -> !providersAfterChange.contains(provider))
                .forEach(provider -> {
                    changeset.beforeChange(provider);
                    ((TopologyStitchingEntity)provider).removeConsumer(entityToUpdate);
                });
            // All added providers should now relate to the destination through a consumer relationship.
            providersAfterChange.stream()
                .filter(provider -> !providersBeforeChangeCopy.contains(provider))
                .forEach(provider -> {
                    changeset.beforeChange(provider);
                    ((TopologyStitchingEntity)provider).addConsumer(entityToUpdate);
                });

            connectedToBeforeChangeCopy.forEach((connectionType, beforeEntities) -> {
                final Set<StitchingEntity> afterChangeEntities =
                    connectedToAfterChange.getOrDefault(connectionType, Collections.emptySet());

                // Remove all removed connections from the destination.
                beforeEntities.forEach(beforeEntity -> {
                    if (!afterChangeEntities.contains(beforeEntity)) {
                        changeset.beforeChange(beforeEntity);
                        ((TopologyStitchingEntity)beforeEntity).removeConnectedFrom(connectionType, entityToUpdate);
                    }
                });

                // Add all added connections to the destination.
                afterChangeEntities.forEach(afterEntity -> {
                    if (!beforeEntities.contains(afterEntity)) {
                        changeset.beforeChange(afterEntity);
                        ((TopologyStitchingEntity)afterEntity).addConnectedFrom(connectionType, entityToUpdate);
                    }
                });
            });
        }
    }

    /**
     * A stitching change that makes no changes to relationships on any entity in the topology.
     * This sort of change may update the builder or the values in some commodity on a single entity.
     */
    public static class UpdateEntityAloneChange<ENTITY extends JournalableEntity<ENTITY>>
        extends BaseTopologicalChange<ENTITY> {
        private final ENTITY entityToUpdate;
        private final Consumer<ENTITY> updateMethod;

        public UpdateEntityAloneChange(@Nonnull final ENTITY entityToUpdate,
                                       @Nonnull final Consumer<ENTITY> updateMethod) {
            this.entityToUpdate = Objects.requireNonNull(entityToUpdate);
            this.updateMethod = Objects.requireNonNull(updateMethod);
        }

        @Override
        protected String getPreamble() {
            return "Updating entity properties for " + entityToUpdate.getJournalableSignature();
        }

        @Override
        protected void applyChangeInternal(@Nonnull final JournalChangeset<ENTITY> changeset) {
            changeset.beforeChange(entityToUpdate);
            updateMethod.accept(entityToUpdate);
        }
    }
}
