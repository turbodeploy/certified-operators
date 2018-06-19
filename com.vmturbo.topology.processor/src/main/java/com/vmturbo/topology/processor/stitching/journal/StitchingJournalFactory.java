package com.vmturbo.topology.processor.stitching.journal;

import java.time.Clock;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AtomicDouble;

import com.vmturbo.common.protobuf.topology.Stitching.EntityFilter;
import com.vmturbo.common.protobuf.topology.Stitching.JournalOptions;
import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.journal.JournalFilter;
import com.vmturbo.stitching.journal.JournalFilter.FilterByEntity;
import com.vmturbo.stitching.journal.JournalFilter.IncludeAllFilter;
import com.vmturbo.stitching.journal.JournalRecorder;
import com.vmturbo.stitching.journal.JournalRecorder.LoggerRecorder;
import com.vmturbo.stitching.journal.JournalableEntity;
import com.vmturbo.stitching.journal.SemanticDiffer;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;

/**
 * Creates stitching journals for use in tracing changes made to the topology during stitching.
 */
public interface StitchingJournalFactory {

    /**
     * Create a journal for use when executing pre-stitching and main-stitching phases (operates on
     * {@link StitchingEntity} types.
     *
     * Request a child journal from the created stitching journal for an appropriate journal to
     * track post-stitching related changes.
     *
     * @param stitchingContext The context containing the entities to be stitched. May be used in the
     *                         initialization of the journal.
     *
     * @return a journal for use when executing pre- and main-stitching phases.
     */
    IStitchingJournal<StitchingEntity> stitchingJournal(@Nullable final StitchingContext stitchingContext);

    /**
     * Create a new {@link ConfigurableStitchingJournalFactory}.
     *
     * @param clock The clock to use for timings.
     * @return a new {@link ConfigurableStitchingJournalFactory}.
     */
    static ConfigurableStitchingJournalFactory configurableStitchingJournalFactory(
        @Nonnull final Clock clock) {
        return new ConfigurableStitchingJournalFactory(clock);
    }

    /**
     * Create a new {@link EmptyStitchingJournalFactory}.
     * Prefer when there is no need to record changes to allow for optimum performance.
     *
     * @return a new {@link EmptyStitchingJournalFactory}.
     */
    static EmptyStitchingJournalFactory emptyStitchingJournalFactory() {
        return new EmptyStitchingJournalFactory();
    }

    /**
     * Create a new {@link RandomEntityStitchingJournalFactory}.
     * For every journalsPerRecording topology broadcasts, we will record to the stitching journal once.
     * Increasing this number decreases the frequency of recording to the journal. When we do choose to
     * record to the journal, we will record stitching change information for at maximum
     * numEntitiesToRecord entities.
     *
     * Choose a low number of numEntitiesToRecord to create a {@link StitchingJournalFactory} suitable
     * for use in production-like environments even with extremely large topologies.
     *
     * See comments for {@link RandomEntitySelector} for how the factory selects entities to trace in the journal
     * when it decides to create a non-empty journal.
     *
     * @param clock The clock to use for timings.
     * @param numEntitiesToRecord The number of entities to record to the journal when we choose to record to the
     *                            journal. Setting this number to a low amount allows us to observe stitching behavior
     *                            for a small selection of entities even in very large production environments.
     * @param maxChangesetsPerOperation To prevent accidentally stalling out the topology pipeline for an extremely
     *                                  long period of time collecting journal entries for a large topology, when
     *                                  an operation enters more than this number of changesets into the journal,
     *                                  we do not include any further changesets produced by this operation in
     *                                  the journal.
     * @param journalsPerRecording For every journalsPerRecording journals requested from this factory, we will
     *                             record to the stitching journal once. Increasing this number decreases
     *                             the frequency of recording to the journal. For example, if this number is twelve
     *                             the factory will generate a journal that records information about a random
     *                             selection of entities once for every twelve times it is asked for a journal.
     *                             The other eleven times, it will produce a {@link EmptyStitchingJournal}.
     * @return A new {@link RandomEntityStitchingJournalFactory}.
     */
    static RandomEntityStitchingJournalFactory randomEntityStitchingJournalFactory(@Nonnull final Clock clock,
                                                                                   final int numEntitiesToRecord,
                                                                                   final int maxChangesetsPerOperation,
                                                                                   final int journalsPerRecording) {
        return new RandomEntityStitchingJournalFactory(clock, numEntitiesToRecord,
            maxChangesetsPerOperation, journalsPerRecording);
    }

    /**
     * An implementation of the {@link StitchingJournalFactory} interface that permits on-demand
     * configuration of the options and filters to apply to generated {@link IStitchingJournal}s.
     */
    class ConfigurableStitchingJournalFactory implements StitchingJournalFactory {

        private final Set<JournalRecorder> recorders = new HashSet<>();

        private JournalFilter filter = new IncludeAllFilter();

        private JournalOptions journalOptions = JournalOptions.getDefaultInstance();

        private final Clock clock;

        /**
         * Create a new {@link StitchingJournalFactory} for use in generating {@link StitchingJournal} instances.
         */
        private ConfigurableStitchingJournalFactory(@Nonnull final Clock clock) {
            this.clock = Objects.requireNonNull(clock);
        }

        public IStitchingJournal<StitchingEntity> stitchingJournal(@Nullable final StitchingContext stitchingContext) {
            final SemanticDiffer<StitchingEntity> semanticDiffer = new StitchingEntitySemanticDiffer(
                journalOptions.getVerbosity());
            return new StitchingJournal<>(filter, journalOptions, semanticDiffer, recorders, clock);
        }

        /**
         * Add a recorder to the list of recorders to be used by journals created via this factory.
         *
         * @param recorder A recorder to be used to record changes to the topology entered into journals
         *                 created by this factory.
         * @return A reference to {@link this} to support method chaining.
         */
        public ConfigurableStitchingJournalFactory addRecorder(@Nonnull final JournalRecorder recorder) {
            recorders.add(recorder);
            return this;
        }

        /**
         * Get an unmodifiable view of the recorders added to the factory.
         *
         * @return an unmodifiable view of the recorders added to the factory.
         */
        public Set<JournalRecorder> getRecorders() {
            return Collections.unmodifiableSet(recorders);
        }

        /**
         * Remove a recorder. Returns false if the recorder cannot be removed.
         *
         * @param recorder The recorder to remove.
         * @return true if the recorder was known and was removed, false if the recorder is unknown and not removed.
         */
        public boolean removeRecorder(@Nonnull final JournalRecorder recorder) {
            return recorders.remove(recorder);
        }

        /**
         * Get the filter to be used by journals created by this factory.
         *
         * @return The filter to be used by journals created by this factory.
         */
        public JournalFilter getFilter() {
            return filter;
        }

        /**
         * Set the filter to be used by journals created by this factory.
         *
         * @param filter the filter to be used by journals created by this factory.
         * @return A reference to {@link this} to support method chaining.
         */
        public ConfigurableStitchingJournalFactory setFilter(@Nonnull final JournalFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Get the options to be used in configuring stitching journals created by this factory.
         *
         * @return the options to be used in configuring stitching journals created by this factory.
         */
        public JournalOptions getJournalOptions() {
            return journalOptions;
        }

        /**
         * Set the options to be used in configuring stitching journals created by this factory.
         *
         * @param journalOptions the options to be used in configuring stitching journals created by this factory.
         * @return A reference to {@link this} to support method chaining.
         */
        public ConfigurableStitchingJournalFactory setJournalOptions(@Nonnull final JournalOptions journalOptions) {
            this.journalOptions = Objects.requireNonNull(journalOptions);
            return this;
        }
    }

    /**
     * An implementation of the {@link StitchingJournalFactory} interface that always returns
     * an {@link EmptyStitchingJournal} when there is no need to trace the changes made by stitching.
     */
    class EmptyStitchingJournalFactory implements StitchingJournalFactory {

        @Override
        public IStitchingJournal<StitchingEntity> stitchingJournal(@Nullable final StitchingContext stitchingContext) {
            return new EmptyStitchingJournal<>();
        }
    }

    /**
     * This journal automatically adds a {@link LoggerRecorder} to every created journal's recorders.
     */
    @ThreadSafe
    class RandomEntityStitchingJournalFactory implements StitchingJournalFactory {

        private final JournalOptions.Builder journalOptionsBuilder = JournalOptions
            .newBuilder()
            .setVerbosity(Verbosity.LOCAL_CONTEXT_VERBOSITY);

        private final int numEntitiesToRecord;

        private final int journalsPerRecording;

        private final int maxChangesetsPerOperation;

        private final AtomicInteger journalCount;

        private final Clock clock;

        private static final Logger logger = LogManager.getLogger();

        public RandomEntityStitchingJournalFactory(@Nonnull final Clock clock,
                                                   final int numEntitiesToRecord,
                                                   final int maxChangesetsPerOperation,
                                                   final int journalsPerRecording) {
            Preconditions.checkArgument(numEntitiesToRecord >= 0);
            Preconditions.checkArgument(maxChangesetsPerOperation >= 0);
            Preconditions.checkArgument(journalsPerRecording >= 1,
                "journalsPerRecording must be at least one. When set to 1, every broadcast will be recorded. " +
                    "Higher numbers produce a lower frequency of recording.");
            if (maxChangesetsPerOperation > 500 && numEntitiesToRecord > 250) {
                logger.warn("The provided configurations may introduce noticeable performance overhead " +
                    "on the topology broadcast.");
            }

            this.numEntitiesToRecord = numEntitiesToRecord;
            this.maxChangesetsPerOperation = maxChangesetsPerOperation;
            this.journalsPerRecording = journalsPerRecording;
            this.clock = Objects.requireNonNull(clock);
            this.journalCount = new AtomicInteger();
        }

        @Override
        public IStitchingJournal<StitchingEntity> stitchingJournal(
            @Nullable final StitchingContext stitchingContext) {
            if (stitchingContext == null) {
                logger.error("Unable to choose random entities to record in the journal. " +
                    "Please provide a non-null StitchingContext. Proceeding with an empty journal.");
                return new EmptyStitchingJournal<>();
            }

            if (journalCount.incrementAndGet() % journalsPerRecording != 0) {
                // Only record journal entries once for each journalsPerRecording times a journal
                // is requested from this factory.
                return new EmptyStitchingJournal<>();
            }

            final Set<TopologyStitchingEntity> entitySelections =
                new RandomEntitySelector(stitchingContext).select(numEntitiesToRecord);
            final JournalFilter filter = new FilterByEntity(EntityFilter.newBuilder()
                .addAllOids(entitySelections.stream()
                    .map(TopologyStitchingEntity::getOid)
                    .collect(Collectors.toList()))
                .build());

            final JournalOptions journalOptions = journalOptionsBuilder
                .setMaxChangesetsPerOperation(maxChangesetsPerOperation)
                .build();
            final SemanticDiffer<StitchingEntity> semanticDiffer = new StitchingEntitySemanticDiffer(
                journalOptions.getVerbosity());
            final StitchingJournal<StitchingEntity> journal
                = new StitchingJournal<>(filter, journalOptions, semanticDiffer,
                Collections.singleton(new LoggerRecorder()), clock);

            if (!entitySelections.isEmpty()) {
                journal.recordMessage("Tracing stitching changes to the following entities: " +
                    entitySelections.stream()
                    .map(JournalableEntity::getJournalableSignature)
                    .collect(Collectors.toList()));
            }

            return journal;
        }
    }

    /**
     * A helper for selecting random entities from a {@link StitchingContext}.
     *
     * The idea is that the rules for stitching a specific type of entity for a specific target are
     * generally quite similar regardless of which entity it is.
     *
     * So in order to collect information about a selection of entities without introducing a
     * performance impact in production, we randomly select entities across a spectrum of
     * target-entity combinations.
     *
     * We apply a slight weighting so that if, for example, there are 10_000 VMs discovered by VC
     * and only 2 StorageControllers discovered by NetApp, we are slightly more likely to choose
     * to journal changes for a VC VM than for a NetApp StorageController. However, we calculate
     * this weighting based on a logarithmic scale of the number of entities so we still have
     * a fair chance of, at some point, choosing to include a journal entry for a NetApp
     * StorageController in the example above.
     *
     * It is possible to select the same entity multiple times, but for a large topology this is
     * unlikely. Selecting the same entity multiple times has no impact other than that we
     * include one entity less in the journal because the entities selected are treated as a set.
     */
    class RandomEntitySelector {
        private final StitchingContext stitchingContext;

        private final Random random;

        private static final double LOGARITHMIC_BASE = 1.5;

        private static final double LOGARITHMIC_DIVISOR = Math.log(LOGARITHMIC_BASE);

        RandomEntitySelector(StitchingContext stitchingContext) {
            this(stitchingContext, new Random());
        }

        RandomEntitySelector(@Nonnull final StitchingContext stitchingContext,
                             @Nonnull final Random random) {
            this.stitchingContext = Objects.requireNonNull(stitchingContext);
            this.random = Objects.requireNonNull(random);
        }

        /**
         * Choose numEntitiesToSelect entities at random, trying to select a variety of different
         * entity types, weighting entity type choices based on how many entities of that type there
         * are.
         *
         * @param numEntitiesToSelect The number of entities to select.
         * @return Selected entities
         */
        public Set<TopologyStitchingEntity> select(int numEntitiesToSelect) {
            final List<TargetEntitySectionWeight> probabilities = getTargetEntitySectionWeights();

            if (probabilities.size() == 0) {
                return Collections.emptySet(); // No entities to choose.
            }
            final double randomChoiceCeiling = probabilities.get(probabilities.size() - 1)
                .sectionWeightTop;

            /**
             * Choose a random number below the top-end of the range of weights generated
             * above. We create a "section" for each target+entityType combination and generate
             * a weight for the likelihood of choosing an entity of the entity type discovered
             * by the given target.
             *
             * So for example if we have a distribution that looks like:
             *
             * +----------+----------+----------+
             * | VM 20.5  | PM 35.8  | ST 55.1  |
             * +----------+----------+----------+
             *
             * we choose a number between 0 and 55.1 (the top end).
             * We iterate the ranges and the first time we find a number higher than the random number,
             * we pick that entity type. So in the above example, if we pick the random number 10.1 we
             * will pick a VM, and if we pick the number 23.4 we will choose a PM.
             */
            return IntStream.range(0, numEntitiesToSelect)
                .mapToObj(i -> selectTargetAndEntityType(probabilities, random.nextDouble() * randomChoiceCeiling))
                .map(this::selectEntityFromTargetAndEntityType)
                .filter(entity -> entity != null)
                .collect(Collectors.toSet());
        }

        /**
         * Build up a list of {@link TargetEntitySectionWeight}s. The probability of selecting a
         * given entity/target pair is weighted by 1 + log(# of entities of that type and
         * target).
         */
        @Nonnull
        @VisibleForTesting
        List<TargetEntitySectionWeight> getTargetEntitySectionWeights() {
            final AtomicDouble nextSectionWeightTop = new AtomicDouble(0);

            return stitchingContext.getEntitiesByEntityTypeAndTarget().entrySet().stream()
                .flatMap(targetsMapEntry -> targetsMapEntry.getValue().entrySet().stream()
                    .filter(targetMap -> targetMap.getValue().size() > 0)
                    .map(targetMap -> {
                        final double sectionWeightTop = nextSectionWeightTop
                            .addAndGet(1.0 + logarithmFor(targetMap.getValue().size()));
                        return new TargetEntitySectionWeight(
                            targetsMapEntry.getKey(), targetMap.getKey(), sectionWeightTop);
                    }))
            .collect(Collectors.toList());
        }

        /**
         * Select at random an entity of the given type discovered by the given target.
         *
         * @param targetEntitySectionWeight Choose an entity of the type and discovered by the target
         *                                given in the probability object.
         * @return An entity of the type and discovered by the given target. If it is not possible
         *         to select such an entity, returns null.
         */
        @Nullable
        private TopologyStitchingEntity selectEntityFromTargetAndEntityType(
            @Nonnull final TargetEntitySectionWeight targetEntitySectionWeight) {
            final Map<Long, List<TopologyStitchingEntity>> entitiesByTarget =
                stitchingContext.getEntitiesByEntityTypeAndTarget().get(targetEntitySectionWeight.entityType);

            if (entitiesByTarget == null) {
                return null;
            } else {
                final List<TopologyStitchingEntity> entities = entitiesByTarget.get(targetEntitySectionWeight.targetId);
                if (entities == null || entities.size() == 0) {
                    return null;
                }
                return entities.get(random.nextInt(entities.size()));
            }
        }

        @Nonnull
        private TargetEntitySectionWeight selectTargetAndEntityType(
            @Nonnull final List<TargetEntitySectionWeight> probabilities, final double randomValueChoice) {
            Preconditions.checkArgument(probabilities.size() > 0);

            for (TargetEntitySectionWeight probability : probabilities) {
                if (probability.sectionWeightTop >= randomValueChoice) {
                    return probability;
                }
            }

            // On the off-chance we didn't find a ceiling above the selected choice, return the last item.
            return probabilities.get(probabilities.size() - 1);
        }

        private double logarithmFor(final int x) {
            // Calculate the logarithm of size in the base LOGARITHMIC_BASE.
            // Take advantage of the change-of-base formula that log_b(x) = log_e(x) / log_e(b)
            return Math.log(x) / LOGARITHMIC_DIVISOR;
        }

        /**
         * A helper that bundles together an EntityType, a targetId, and a probability weighting.
         * See comments in {@link RandomEntitySelector#select(int)} for an explanation of how
         * these section weights are used.
         */
        @VisibleForTesting
        static class TargetEntitySectionWeight {
            public final EntityType entityType;
            public final long targetId;
            public final double sectionWeightTop;

            private TargetEntitySectionWeight(@Nonnull final EntityType entityType,
                                              final long targetId,
                                              final double sectionWeightTop) {
                this.entityType = entityType;
                this.targetId = targetId;
                this.sectionWeightTop = sectionWeightTop;
            }
        }
    }
}